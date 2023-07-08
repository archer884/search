use std::{
    borrow::Cow,
    collections::HashMap,
    env,
    ffi::OsStr,
    fs::{self, File},
    io,
    ops::Deref,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use scraper::Html;
use serde::{Deserialize, Serialize};
use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    doc,
    query::QueryParser,
    schema::{self, Field, Schema},
    Index,
};

#[derive(Clone, Debug, Parser)]
#[clap(version, subcommand_negates_reqs(true))]
struct Args {
    #[clap(required = true)]
    query: Vec<String>,

    #[clap(short, long)]
    open: bool,

    /// index name
    ///
    /// Search a named library instead of guessing the library name based on the current working
    /// directory.
    #[clap(short, long)]
    index: Option<String>,

    #[clap(flatten)]
    skip_take: SkipTake,

    #[clap(subcommand)]
    command: Option<Command>,
}

impl Args {
    fn query_string(&self) -> String {
        if self.query.is_empty() {
            return String::new();
        }

        let mut buf = String::from(&self.query[0]);
        for part in &self.query[1..] {
            buf += " ";
            buf += part;
        }

        buf
    }

    fn skip_take(&self) -> (Skip, Take) {
        (
            self.skip_take.skip.unwrap_or_default().into(),
            self.skip_take
                .take
                .unwrap_or(10)
                .into(),
        )
    }
}

struct Skip(usize);

impl From<usize> for Skip {
    fn from(n: usize) -> Self {
        Self(n)
    }
}

impl Deref for Skip {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct Take(usize);

impl From<usize> for Take {
    fn from(n: usize) -> Self {
        Self(n)
    }
}

impl Deref for Take {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, Subcommand)]
enum Command {
    /// create a new index
    #[clap(alias = "ci")]
    CreateIndex(IndexCmd),

    /// list indexes
    #[clap(alias = "ls")]
    ListIndexes,

    /// update index
    #[clap(alias = "u")]
    Update,
}

trait IndexArgs {
    fn name(&self) -> &str;
    fn root(&self) -> io::Result<Cow<Path>>;
    fn force(&self) -> bool;
}

// FIXME: change IndexCmd to eliminate the error case so that we can simplify the trait

#[derive(Clone, Debug, Parser)]
struct IndexCmd {
    /// library name
    ///
    /// Each search library needs a name so that we have a place to store the index.
    name: String,

    /// library root
    ///
    /// The location of the files to be indexed. (Defaults to current directory.)
    root: Option<String>,

    /// overwrite existing index
    ///
    /// If search finds an existing index in the intended library location, the indexing process
    /// will be aborted. Pass this flag to force reindexing.
    #[clap(short, long)]
    force: bool,
}

impl IndexArgs for IndexCmd {
    fn name(&self) -> &str {
        &self.name
    }

    fn root(&self) -> io::Result<Cow<Path>> {
        match self.root.as_deref() {
            Some(path) => Ok(Cow::from(Path::new(path))),
            None => env::current_dir().map(Cow::from),
        }
    }

    fn force(&self) -> bool {
        self.force
    }
}

struct UpdateCmd<'a> {
    name: &'a str,
    root: &'a Path,
}

impl IndexArgs for UpdateCmd<'_> {
    fn name(&self) -> &str {
        self.name
    }

    fn root(&self) -> io::Result<Cow<Path>> {
        Ok(self.root.into())
    }

    fn force(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug, Parser)]
struct SkipTake {
    #[clap(short, long)]
    skip: Option<usize>,

    #[clap(short, long)]
    take: Option<usize>,
}

struct SearchFields {
    /// file system path
    path: Field,

    /// author name/title as a facet
    // byline: Field,

    /// text
    text: Field,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[repr(transparent)]
struct Libraries {
    mapping: HashMap<PathBuf, String>,
}

impl Libraries {
    fn from_path(path: &Path) -> io::Result<Libraries> {
        let path = if path.ends_with("libraries.json") {
            Cow::from(path)
        } else {
            Cow::from(path.join("libraries.json"))
        };

        if !path.exists() {
            return Ok(Default::default());
        }

        let text = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&text)?)
    }

    // fn get_index_name<'a>(&'a self, args: &'a Args) -> io::Result<&'a str> {
    fn get_index_name<'a>(&'a self, path: &Path) -> io::Result<&'a str> {
        Ok(self
            .mapping
            .get(path)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no library for {}", path.display()),
                )
            })?
            .as_ref())
    }
}

fn main() {
    if let Err(e) = run(&Args::parse()) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run(args: &Args) -> anyhow::Result<()> {
    if let Some(command) = &args.command {
        return dispatch(command);
    }

    // It is not valid to perform a search if no index is available, so the first thing we'll do
    // is check to see that there's a valid index to search. We can do this on the basis of an
    // index name or on the basis of the current working directory.

    let storage_path = get_storage_path()?;
    let libraries = Libraries::from_path(&storage_path)?;
    let name = match args.index.as_deref() {
        Some(name) => name,
        None => libraries.get_index_name(&env::current_dir()?)?,
    };

    let (_schema, fields) = build_schema();
    let index = Index::open(MmapDirectory::open(storage_path.join(name))?)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, vec![fields.text]);
    let query = parser.parse_query(&args.query_string())?;

    let (skip, take) = args.skip_take();
    let texts = searcher.search(&query, &TopDocs::with_limit(*take).and_offset(*skip))?;
    let texts = texts.into_iter().filter_map(|(_, doc_id)| {
        searcher
            .doc(doc_id)
            .ok()?
            .get_first(fields.path)?
            .as_text()
            .map(ToOwned::to_owned)
    });

    if args.open {
        let mut state = false;
        for path in texts {
            if state {
                thread::sleep(Duration::from_millis(500));
            } else {
                state = true;
            }
            open::that(path)?;
        }
    } else {
        for path in texts {
            println!("{path}");
        }
    }

    Ok(())
}

fn dispatch(command: &Command) -> anyhow::Result<()> {
    match command {
        Command::CreateIndex(args) => build_index(args),
        Command::ListIndexes => list_indexes(),
        Command::Update => update_index(),
        // FIXME: add command for requesting the index for the current dir
    }
}

fn update_index() -> anyhow::Result<()> {
    let storage_path = get_storage_path()?;
    let libraries = Libraries::from_path(&storage_path)?;
    let root = env::current_dir()?;
    let name = libraries.get_index_name(&root)?;

    build_index(&UpdateCmd {
        root: &root,
        name: &name,
    })
}

fn list_indexes() -> anyhow::Result<()> {
    let storage_path = get_storage_path()?;
    let libraries = Libraries::from_path(&storage_path)?;

    let mut libraries: Vec<_> = libraries.mapping.iter().collect();
    libraries.sort_unstable_by(|a, b| a.1.cmp(&b.1));

    for (path, name) in libraries {
        println!("{name}\n  {}", path.display());
    }

    Ok(())
}

fn build_index(args: &impl IndexArgs) -> anyhow::Result<()> {
    // To build our index is actually a two-step process. First, we actually need to register the
    // library in our library mappings, because we need some way to know which library we are
    // searching. Before that (so zerost, I guess) we need to actually create the index, beacuse
    // there is no point in registering a library for an index that we failed to build to begin
    // with.

    let root = args.root()?;
    let storage_path = get_storage_path()?;

    initialize(args, &storage_path, &root)?;

    // Registration starts here. The first thing we need to concern ourselves about is whether or
    // not a library with the given name is already registered. If so, we'll either return here
    // or continue depending on whether or not the force flag has been set.

    update_registry(storage_path, args, &*root)?;

    Ok(())
}

fn update_registry(
    storage_path: PathBuf,
    args: &impl IndexArgs,
    root: &Path,
) -> Result<(), anyhow::Error> {
    let registry = storage_path.join("libraries.json");
    let libraries = Libraries::from_path(&storage_path)?;

    if libraries.mapping.values().any(|val| val == &args.name()) && !args.force() {
        let name = args.name();
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("library {name:?} is already registered"),
        )
        .into());
    }

    let mut mapping: HashMap<_, _> = libraries
        .mapping
        .into_iter()
        .filter(|(_key, value)| value != &args.name())
        .collect();
    mapping.insert(root.to_owned(), args.name().to_owned());

    let libraries = Libraries { mapping };
    serde_json::to_writer_pretty(&mut File::create(&registry)?, &libraries)?;
    Ok(())
}

fn initialize(
    args: &impl IndexArgs,
    storage_path: &Path,
    root: &Path,
) -> Result<(), anyhow::Error> {
    static MEMORY: usize = 0xC800000; // 100 megs?
    static BATCH_SIZE: usize = 20_000;

    let data_path = get_data_path(args, storage_path)?;
    let (schema, fields) = build_schema();
    let index = Index::create_in_dir(&data_path, schema)?;

    let mut writer = index.writer(MEMORY)?;
    let mut count = 0;

    for path in read_paths(root) {
        count += 1;
        if count % BATCH_SIZE == 0 {
            writer.commit()?;
        }

        let data = fs::read(&path)?;
        let text = String::from_utf8_lossy(&data);
        let stored_path = format!("{}", path.display());

        let text = if is_html(&path) {
            let fragment = Html::parse_fragment(&*text);

            let mut buf = String::with_capacity(text.len());
            for s in fragment.root_element().text() {
                buf += " ";
                buf += s.trim();
            }

            buf
        } else {
            text.to_string()
        };

        writer.add_document(doc! {
            fields.path => stored_path,
            fields.text => text,
        })?;
    }

    writer.commit()?;

    Ok(())
}

fn read_paths(root: &Path) -> impl Iterator<Item = PathBuf> {
    // This is a starter set. We'll need more, I'm sure.
    static EXTENSIONS: &[&str] = &["html", "htm", "txt"];

    walkdir::WalkDir::new(root).into_iter().filter_map(|entry| {
        let entry = entry.ok()?;
        let path = entry.path();
        let extension = path.extension()?;

        if path.is_file()
            && EXTENSIONS
                .iter()
                .copied()
                .any(|ext| OsStr::new(ext) == extension)
        {
            Some(path.into())
        } else {
            None
        }
    })
}

fn build_schema() -> (Schema, SearchFields) {
    let mut builder = Schema::builder();
    let fields = SearchFields {
        path: builder.add_text_field("path", schema::STORED),
        // byline: builder.add_facet_field("byline", schema::INDEXED | schema::STORED),
        text: builder.add_text_field("text", schema::TEXT),
    };
    (builder.build(), fields)
}

fn get_data_path(args: &impl IndexArgs, storage: &Path) -> io::Result<PathBuf> {
    let path = storage.join(args.name());
    let meta = path.join("meta.json");

    if meta.exists() && !args.force() {
        let name = &args.name();
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("an index already exists for library {name:?}"),
        ));
    }

    if path.exists() {
        fs::remove_dir_all(&path)?;
    }

    fs::create_dir_all(&path)?;
    Ok(path)
}

fn get_storage_path() -> io::Result<PathBuf> {
    let dirs = ProjectDirs::from("org", "Hack Commons", "Search-App").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            "unable to initialize project directory",
        )
    })?;

    Ok(dirs.data_dir().into())
}

fn is_html(path: &Path) -> bool {
    static EXTENSIONS: &[&str] = &["htm", "html"];

    path.extension()
        .map(|a| EXTENSIONS.iter().copied().any(|b| a == b))
        .unwrap_or_default()
}
