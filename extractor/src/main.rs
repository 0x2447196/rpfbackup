use rayon::prelude::*;
use rusqlite::{params, Connection, Result};
use scraper::{Html, Selector};
use std::collections::HashSet;
use walkdir::WalkDir;
use clap::Parser;

struct Selectors {
    og_url_selector: Selector,
    title_selector: Selector,
    post_selector: Selector,
    page_selector: Selector,
    datetime_selector: Selector,
    user_selector: Selector,
    avatar_selector: Selector,
    message_body_selector: Selector,
}

impl Selectors {
    fn new() -> Self {
        Selectors {
            og_url_selector: Selector::parse(r#"meta[property="og:url"]"#).unwrap(),
            title_selector: Selector::parse("h1.p-title-value").unwrap(),
            post_selector: Selector::parse("article.message--post").unwrap(),
            page_selector: Selector::parse(".pageNav-page--current").unwrap(),
            datetime_selector: Selector::parse(".message-header time").unwrap(),
            user_selector: Selector::parse("a.username").unwrap(),
            avatar_selector: Selector::parse("span.avatar").unwrap(),
            message_body_selector: Selector::parse("article.message-body").unwrap(),
        }
    }
}

#[derive(Debug)]
struct PostData {
    post_id: i32,
    user_id: i32,
    username: String,
    thread_order: i32,
    datetime: String,
    message_body: String,
}

struct ForumThreadData {
    thread_slug: String,
    thread_id: u64,
    thread_title: String,
    posts: Vec<PostData>,
}

impl ForumThreadData {
    // Function to save ForumThreadData to the database
    fn save_to_db(&self, conn: &Connection) -> Result<()> {
        conn.execute(
            "INSERT OR IGNORE INTO threads (id, title, slug) VALUES (?1, ?2, ?3)",
            params![self.thread_id, self.thread_title, self.thread_slug],
        )?;

        let mut user_ids = HashSet::new();

        for post in &self.posts {
            // Insert user if not already processed
            if user_ids.insert(post.user_id) {
                conn.execute(
                    "INSERT OR IGNORE INTO users (id, name) VALUES (?1, ?2)",
                    params![post.user_id, post.username],
                )?;
            }

            // Insert post
            conn.execute(
                "INSERT OR IGNORE INTO posts (id, user_id, thread_id, thread_order, datetime, content)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    post.post_id,
                    post.user_id,
                    self.thread_id,
                    post.thread_order,
                    post.datetime,
                    post.message_body.trim()
                ],
            )?;
        }

        Ok(())
    }
}

const POSTS_PER_PAGE: usize = 15;

fn process_file(html_content: &str, selectors: &Selectors) -> ForumThreadData {
    let document = Html::parse_document(html_content);

    // Find the specific <meta> element
    let og_url = document
        .select(&selectors.og_url_selector)
        .next()
        .expect("No <meta property=\"og:url\"> tag found")
        .value()
        .attr("content")
        .expect("<meta property=\"og:url\"> tag has no content");

    let parts: Vec<&str> = og_url.split('/').collect();
    let full_slug = parts[parts.len() - 2];
    let slug_parts: Vec<&str> = full_slug.split('.').collect();
    let thread_slug = slug_parts[0].to_string();
    let thread_id_str = slug_parts[1];
    let thread_id = thread_id_str
        .parse::<u64>()
        .expect("Failed to parse thread_id");

    let thread_title = document
        .select(&selectors.title_selector)
        .next()
        .expect("No <h1 class=\"p-title-value\"> tag found")
        .inner_html()
        .trim()
        .to_string();

    // Find all posts
    let all_posts = document.select(&selectors.post_selector);

    // Find the current page number
    let page_num = document
        .select(&selectors.page_selector)
        .next()
        .map(|page| page.inner_html().parse::<i32>().unwrap_or(0))
        .unwrap_or(0);

    let mut post_data = Vec::new();

    for (i, post) in all_posts.enumerate() {
        let data_content = post.value().attr("data-content").unwrap();
        let post_id = data_content
            .split('-')
            .nth(1)
            .unwrap()
            .parse::<i32>()
            .unwrap();

        let datetime = post
            .select(&selectors.datetime_selector)
            .next()
            .expect("No <time> tag in .message-header")
            .value()
            .attr("datetime")
            .unwrap();

        let user = post.select(&selectors.user_selector).next();
        let (user_id, username) = if let Some(user) = user {
            (
                user.value()
                    .attr("data-user-id")
                    .unwrap()
                    .parse::<i32>()
                    .unwrap(),
                user.text().collect::<Vec<_>>().join(" "),
            )
        } else {
            let user = post
                .select(&selectors.avatar_selector)
                .next()
                .expect("No user element found");
            (0, user.value().attr("title").unwrap().to_string())
        };

        let message_body = post
            .select(&selectors.message_body_selector)
            .next()
            .expect("No <article class=\"message-body\"> tag found")
            .inner_html();

        post_data.push(PostData {
            post_id,
            user_id,
            username,
            thread_order: page_num * POSTS_PER_PAGE as i32 + i as i32,
            datetime: datetime.to_string(),
            message_body,
        });
    }

    ForumThreadData {
        thread_slug,
        thread_id,
        thread_title,
        posts: post_data,
    }
}

/// Forum Data Processor
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Sets a custom database file
    #[arg(short, long, default_value = "forum_data.db")]
    db_path: String,

    /// Sets the input folder of HTML files
    #[arg(required = true)]
    input_folder: String,
}

fn main() -> Result<()> {


    let args = Args::parse();



    let conn = Connection::open(&args.db_path)?;

    // Create tables if they don't exist
    conn.execute(
        "CREATE TABLE IF NOT EXISTS threads (id INTEGER PRIMARY KEY, title TEXT, slug TEXT)",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY, user_id INTEGER, thread_id INTEGER, thread_order INTEGER, datetime TEXT, content TEXT, FOREIGN KEY(user_id) REFERENCES users(id), FOREIGN KEY(thread_id) REFERENCES threads(id))",
        [],
    )?;

    // Create the selectors once
    let selectors = Selectors::new();
    // Collect all file paths first
    let paths: Vec<_> = WalkDir::new(&args.input_folder)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().map_or(false, |ext| ext == "html")
        })
        .collect();

    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

    pool.install(|| {
        paths.par_iter().for_each(|entry| {
            // Open a new connection for each file/operation
            let mut conn = Connection::open("forum_data.db").expect("Failed to open DB");

            let html_content = std::fs::read_to_string(entry.path()).expect("Error reading file");
            let forum_thread_data = process_file(&html_content, &selectors);

            // Begin a transaction
            let tx = conn.transaction().expect("Failed to start a transaction");
            forum_thread_data
                .save_to_db(&tx)
                .expect("Failed to save data");

            // Commit the transaction
            tx.commit().expect("Failed to commit transaction");
        });
    });

    Ok(())
}
