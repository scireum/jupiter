//! Provides an in-memory database which can be used for ultra fast lookups on static master data.
//!
//! A lot of business related applications need lots of master data like to operate correctly.
//! Most of this data is rather static and large enough to not be "just loaded into the app itself"
//! but also small enough to remain in memory in a dedicated server.
//!
//! Examples would be **code lists** and **mappings** along with **translations** like a list of all
//! packaging units along with mappings for different standards / file formats or a list of
//! customs declaration codes.
//!
//! Basically these datasets are lists of documents as one knows from MongoDB or other "NoSQL"
//! databases but IDB provides some **distinct features** for lookups, reverse lookup, searching
//! or multi language handling.
//!
//! Next to these datasets / code lists (which are internally referred to as "tables") InfoGraphDB
//! also supports "sets" of strings. These sets can be used to efficiently represents lists like
//! "which codes are enabled for standard X" or "which table entries can be used in scenario Y".
//!
//! # Managing data
//!
//! The data stored in IDB is considered "static". Therefore it is loaded once from a source and
//! then cannot be modified anymore. A common source for data is the **Repository** which can
//! use its **Loaders** to transform an input file into one or more tables in IDB. Of course,
//! if the underlying file changes, the file will be re-read and the table will be replaced, IDB
//! just doesn't provide any direct way of manipulating the data.
//!
//! The main idea is to use a common data source like a git repository or a bucket in an object
//! store which contains the master data as **Yaml**, **JSON**, **XML** or other formats. Using the
//! **Repository** commands, this data is then loaded into the database. This has the great benefit
//! that all systems (development, staging, production, customer instances ...) will be
//! automatically update once a file is changed.
//!
//! By default, sets are loaded via the **idb-yaml-sets** loader, which expects a YAML hash which
//! maps one or more keys to lists of set entries like this:
//! ```yaml
//! my_set: [ "A", "B", "C" ]
//! some.other.set: [ "foo", "bar" ]
//! ```
//!
//! Of course, there is also a programmatic API to create or drop tables and sets form other
//! sources.
//!
//! # Commands
//!
//! * **IDB.LOOKUP**: `IDB.LOOKUP table search_path filter_value path1 path2 path3`
//!   Performs a lookup for the given filter value in the given search path (inner fields separated
//!   by ".") within the given table. If a result is found, the values for path1..pathN are
//!   extracted and returned. If no path is given, the number of matches is returned. If multiple
//!   documents match, only the first one if returned. Note that if a path matches an inner object
//!   (which is especially true for "."), the result will be wrapped as JSON.
//! * **IDB.ILOOKUP**: `IDB.ILOOKUP table primary_lang fallback_lang search_path filter_value path1`
//!   Behaves just like `IDB.LOOKUP`. However, of one of the given extraction paths points to an
//!   inner map, we expect this to be a map of translation where we first try to find the value for
//!   the primary language and if none is found for the fallback language. Note that, if both languages
//!   fail to yield a value, we attempt to resolve a final fallback using **xx** as language
//!   code. If all these attempts fail, we output an empty string. Note that therefore it is not
//!   possible to return an inner map when using ILOOKUP which is used for anything other than
//!   translations. Note however, that extracting single values using a proper path still works.
//! * **IDB.QUERY**: `IDB.QUERY table num_skip max_results search_path filter_value path1`
//!   Behaves just like lookup, but doesn't just return the first result, but skips over the first
//!  `num_skip` results and then outputs up to `max_result` rows. Not that this is again limited to
//!   at most **1000**.
//! * **IDB.IQUERY**: `IDB.QUERY table primary_lang fallback_lang num_skip max_results search_path filter_value path1`
//!   Provides essentially the same i18n lookups for `IDB.QUERY` as `IDB.ILOOKUP` does for
//!   `IDB.LOOKUP`.
//! * **IDB.SEARCH**: `IDB.SEARCH table num_skip max_results search_paths filter_value path1`
//!   Performs a search in all fields given as `search_paths`. This can either be comma separated
//!   like "path1,path2,path3" or a "*" to select all fields. Note that for a given search value,
//!   this will match case-insensitive and also for prefixes of a detected word within the
//!   document (the selected fields). Everything else behaves just like `IDB.QUERY`. Also note that
//!   a fulltext index has to be present for each field being queried.
//! * **IDB.ISEARCH**: `IDB.ISEARCH table primary_lang fallback_lang num_skip max_results search_paths filter_value path1`
//!   Adds i18n lookups for the generated results just like `IDB.IQUERY` or `IDB.ILOOKUP`.
//! * **IDB.SCAN**: `IDB.SCAN table num_skip max_results path1 path2 path3`
//!   Outputs all results by skipping over the first `num_skip` entries in the table and then
//!   outputting up to `max_results`rows.
//! * **IDB.ISCAN**: `IDB.ISCAN table primary_lang fallback_lang num_skip max_results path1 path2 path3`
//!   Again, behaves just like `IDB.SCAN` but provides i18n lookup for the given languages.
//! * **IDB.SHOW_TABLES**: `IDB.SHOW_TABLES` reports all tables and their usage statistics.
//! * **IDB.SHOW_SETS**: `IDB.SHOW_SETS` reports all sets and their usage statistics.
//! * **IDB.CONTAINS**: `IDB.CONTAINS set key1 key2 key3` reports if the given keys are contained
//!   in the given set. For each key a **1** (contained) or a **0** (not contained) will be reported.
//! * **IDB.INDEX_OF**: `IDB.INDEX_OF set key1 key2 key3` reports the insertion index for each
//!   of the given keys using one-based indices.
//!    
//! # Example
//!
//! Imagine we have the following super simplified dataset representing some countries:
//! ```yaml
//! code: "D"
//! iso:
//!    two: "de"
//!    three: "deu"
//! name:
//!     de: "Deutschland"
//!     en: "Germany"
//! ---
//! code: "A"
//! iso:
//!    two: "at"
//!    three: "aut"
//! name:
//!     de: "Österreich"
//!     en: "Austria"
//! ```
//!
//! Executing `IDB.LOOKUP countries code D iso.two` would yield "de". We could also use
//! `IDB.ILOOKUP countries de en code D iso.two name` to retrieve "de", "Deutschland" or
//! `IDB.LOOKUP countries name.de Deutschland code` for a reverse lookup yielding "D" again.
//! Note that even `IDB.LOOKUP countries name Deutschland code` would work here, as we
//! index all translations for a field.
//!
//! Note that if both languages given in an `IDB.ILOOKUP` (or `IDB.ISEARCH`, `IDB.ISCAN`) don't
//! yield a value, we check if a final fallback value for the code **xx** is present. This might
//! be usable if there is a default value and only one or a few languages differ.
//!
//! Also note that the whole element being matched can be requested when using "." as field path,
//! e.g. `IDB.LOOKUP countries code D .`. A this matches an inner object, this will return the whole
//! element wrapped as JSON string.
//!
//! We could invoke `IDB.ISEARCH countries en de 0 5 * deutsch code name` to retrieve "D", "Germany"
//! e.g. to provide autocomplete values for a country field.
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::commands::ResultExt;
use crate::commands::{queue, Call, CommandDictionary, CommandError, CommandResult, Endpoint};
use crate::idb::set::Set;
use crate::idb::table::Table;
use crate::ig::docs::{Element, Query};
use crate::request::Request;
use crate::response::Response;
use apollo_framework::fmt::format_size;
use apollo_framework::platform::Platform;

pub mod set;
pub mod table;
pub mod trie;

/// Describes the administrative commands which can be submitted via [Database::perform].
pub enum DatabaseCommand {
    /// Creates (registers) the given table for the given name. If a table already exists with
    /// the same name, it will be replaced.
    CreateTable(String, Table),

    /// Drops (removes) the table with the given name.
    DropTable(String),

    /// Creates (registers) the given set for the given source and name. If a set already exists with
    /// the same name, it will be replaced.
    CreateSet(String, String, Set),

    /// Drops (removes) all sets of the given source.
    DropSets(String),
}

/// Describes the public API of the database.
pub struct Database {
    sender: Sender<DatabaseCommand>,
}

impl Database {
    /// Executes (enqueues) the given administrative command.
    pub async fn perform(&self, command: DatabaseCommand) -> anyhow::Result<()> {
        if self.sender.send(command).await.is_ok() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Failed to enqueue administrative database command."
            ))
        }
    }
}

/// For `IDB.QUERY`, `IDB.SEARCH` and `IDB.SCAN` this limits the maximal number of results being
/// returned for a single command. Use pagination for larger result sets.
const MAX_RESULTS: usize = 1000;

/// Enumerates the commands supported by this facility.
#[derive(FromPrimitive)]
enum Commands {
    ShowTables,
    Query,
    IQuery,
    Lookup,
    ILookup,
    Search,
    ISearch,
    Scan,
    IScan,
    ShowSets,
    Contains,
    IndexOf,
}

/// Installs an actor which handles the commands as described above.
///
/// Also this registers a [Database] in the given platform which permits to submit administrative
/// commands.
pub fn install(platform: Arc<Platform>) {
    let (cmd_queue, cmd_endpoint) = queue();
    let (admin_sender, admin_receiver) = tokio::sync::mpsc::channel(16);
    let database = Arc::new(Database {
        sender: admin_sender,
    });

    platform.register::<Database>(database);

    actor(cmd_endpoint, admin_receiver);

    if let Some(commands) = platform.find::<CommandDictionary>() {
        commands.register_command(
            "IDB.SHOW_TABLES",
            cmd_queue.clone(),
            Commands::ShowTables as usize,
        );

        commands.register_command("IDB.LOOKUP", cmd_queue.clone(), Commands::Lookup as usize);
        commands.register_command("IDB.ILOOKUP", cmd_queue.clone(), Commands::ILookup as usize);
        commands.register_command("IDB.QUERY", cmd_queue.clone(), Commands::Query as usize);
        commands.register_command("IDB.IQUERY", cmd_queue.clone(), Commands::IQuery as usize);
        commands.register_command("IDB.SEARCH", cmd_queue.clone(), Commands::Search as usize);
        commands.register_command("IDB.ISEARCH", cmd_queue.clone(), Commands::ISearch as usize);
        commands.register_command("IDB.SCAN", cmd_queue.clone(), Commands::Scan as usize);
        commands.register_command("IDB.ISCAN", cmd_queue.clone(), Commands::IScan as usize);
        commands.register_command(
            "IDB.SHOW_SETS",
            cmd_queue.clone(),
            Commands::ShowSets as usize,
        );
        commands.register_command(
            "IDB.CONTAINS",
            cmd_queue.clone(),
            Commands::Contains as usize,
        );
        commands.register_command("IDB.INDEX_OF", cmd_queue, Commands::IndexOf as usize);
    }
}

/// Handles both, incoming commands and administrative actions.
fn actor(mut endpoint: Endpoint, mut admin_receiver: Receiver<DatabaseCommand>) {
    let _ = tokio::spawn(async move {
        let mut tables = HashMap::new();
        let mut sets = HashMap::new();

        loop {
            tokio::select! {
                   call = endpoint.recv() => match call {
                        Some(call) => handle_call(call, &tables, &sets).await,
                        None => return
                   },
                   cmd = admin_receiver.recv() => match cmd {
                        Some(cmd) => handle_admin(cmd, &mut tables, &mut sets),
                        None => return
                   }
            }
        }
    });
}

/// Distinguishes table related commands from general ones, as the former can be executed
/// in parallel where the later block the actor to guarantee a sequential execution and to
/// also provide exclusive access on the data.
async fn handle_call(
    mut call: Call,
    tables: &HashMap<String, Arc<Table>>,
    sets: &HashMap<String, (Arc<Set>, String)>,
) {
    let command = Commands::from_usize(call.token);

    match command {
        Some(Commands::ShowTables) => show_tables_command(&mut call, tables).complete(call),
        Some(Commands::ShowSets) => show_sets_command(&mut call, sets).complete(call),
        Some(Commands::Contains) | Some(Commands::IndexOf) => handle_set_call(call, sets).await,
        _ => handle_table_call(call, tables).await,
    }
}

/// Creates a response for `IDB.SHOW_TABLES`.
fn show_tables_command(call: &mut Call, database: &HashMap<String, Arc<Table>>) -> CommandResult {
    if call.request.parameter_count() == 0 {
        let mut result = String::new();

        result += format!(
            "{:<40} {:>10} {:>12} {:>10} {:>12} {:>10}\n",
            "Name", "Num Rows", "Memory", "Queries", "Scan Qrys", "Scans"
        )
        .as_str();
        result += crate::response::SEPARATOR;

        for (name, table) in database {
            result += format!(
                "{:<40} {:>10} {:>12} {:>10} {:>12} {:>10}\n",
                name,
                table.len(),
                format_size(table.allocated_memory()),
                table.num_queries(),
                table.num_scan_queries(),
                table.num_scans()
            )
            .as_str();
        }
        result += crate::response::SEPARATOR;

        call.response.bulk(result)?;
    } else {
        call.response.array(database.len() as i32)?;
        for (name, table) in database {
            call.response.array(6)?;
            call.response.bulk(name)?;
            call.response.number(table.len() as i64)?;
            call.response.number(table.allocated_memory() as i64)?;
            call.response.number(table.num_queries() as i64)?;
            call.response.number(table.num_scan_queries() as i64)?;
            call.response.number(table.num_scans() as i64)?;
        }
    }
    Ok(())
}

/// Creates a response for `IDB.SHOW_SETS`.
fn show_sets_command(
    call: &mut Call,
    database: &HashMap<String, (Arc<Set>, String)>,
) -> CommandResult {
    if call.request.parameter_count() == 0 {
        let mut result = String::new();

        result += format!(
            "{:<40} {:<20} {:>10} {:>12} {:>10}\n",
            "Source", "Name", "Num Elements", "Memory", "Queries"
        )
        .as_str();
        result += crate::response::SEPARATOR;

        for (name, (set, source)) in database {
            result += format!(
                "{:<40} {:<20} {:>10} {:>12} {:>10}\n",
                source,
                name,
                set.len(),
                format_size(set.allocated_memory()),
                set.num_queries()
            )
            .as_str();
        }
        result += crate::response::SEPARATOR;

        call.response.bulk(result)?;
    } else {
        call.response.array(database.len() as i32)?;
        for (name, (set, source)) in database {
            call.response.array(5)?;
            call.response.bulk(source)?;
            call.response.bulk(name)?;
            call.response.number(set.len() as i64)?;
            call.response.number(set.allocated_memory() as i64)?;
            call.response.number(set.num_queries() as i64)?;
        }
    }
    Ok(())
}

/// For a table related call, we perform the lookup in the table dictionary while having
/// exclusive access to the underlying hash map. We then pass the **Arc** reference into
/// a separate thread so that multiple queries can be executed simultaneously.
async fn handle_table_call(mut call: Call, database: &HashMap<String, Arc<Table>>) {
    let table_name = if let Ok(name) = call.request.str_parameter(0) {
        name
    } else {
        call.complete(Err(CommandError::ClientError(anyhow::anyhow!(
            "Missing table name as first parameter!"
        ))));
        return;
    };

    let table = if let Some(table) = database.get(table_name) {
        table.clone()
    } else {
        let table_name = table_name.to_owned();
        call.complete(Err(CommandError::ClientError(anyhow::anyhow!(
            "Unknown table: {}",
            table_name
        ))));
        return;
    };

    let _ = tokio::spawn(async move {
        let token = call.token;
        match Commands::from_usize(token) {
            Some(Commands::Lookup) => {
                execute_query(&mut call, table, false, false, true).complete(call)
            }
            Some(Commands::ILookup) => {
                execute_query(&mut call, table, true, false, true).complete(call)
            }
            Some(Commands::Query) => {
                execute_query(&mut call, table, false, true, true).complete(call)
            }
            Some(Commands::IQuery) => {
                execute_query(&mut call, table, true, true, true).complete(call)
            }
            Some(Commands::Search) => {
                execute_query(&mut call, table, false, true, false).complete(call)
            }
            Some(Commands::ISearch) => {
                execute_query(&mut call, table, true, true, false).complete(call)
            }
            Some(Commands::Scan) => execute_scan(&mut call, table, false).complete(call),
            Some(Commands::IScan) => execute_scan(&mut call, table, true).complete(call),
            _ => call.complete(Err(CommandError::ServerError(anyhow::anyhow!(
                "Unknown token received: {}!",
                token
            )))),
        }
    });
}

/// Based on the `translate`, `read_limits` and `exact` settings here, this will yield a response
/// to either `IDB.(I)LOOKUP`, `IDB.(I)QUERY` and `IDB.(I)SEARCH`.
fn execute_query(
    call: &mut Call,
    table: Arc<Table>,
    translate: bool,
    read_limits: bool,
    exact: bool,
) -> CommandResult {
    let mut parameter_index = 1;
    let (primary_lang, fallback_lang) = parse_langs(call, &table, translate, &mut parameter_index)?;
    let (skip, limit) = if read_limits {
        parse_limits(call, &mut parameter_index)?
    } else {
        (0, 1)
    };

    let query = call
        .request
        .str_parameter(parameter_index)
        .context("Missing query parameter.")?;
    parameter_index += 1;
    let value = call
        .request
        .str_parameter(parameter_index)
        .context("Missing value parameter.")?;
    parameter_index += 1;

    let mut iter = table.query(query, value, exact)?.skip(skip as usize);

    emit_results(
        &call.request,
        &mut call.response,
        &table,
        parameter_index,
        &mut iter,
        limit as usize,
        I18nContext {
            primary_lang,
            fallback_lang,
            default_lang: table.default_lang_query(),
        },
    )?;

    Ok(())
}

struct I18nContext<'a> {
    primary_lang: Option<Query>,
    fallback_lang: Option<Query>,
    default_lang: &'a Query,
}

/// Yields an appropriate result for `IDB.SCAN` or `IDB.ISCAN` (depending on the value of
/// `translate`).
fn execute_scan(call: &mut Call, table: Arc<Table>, translate: bool) -> CommandResult {
    let mut parameter_index = 1;
    let (primary_lang, fallback_lang) = parse_langs(call, &table, translate, &mut parameter_index)?;
    let (skip, limit) = if parameter_index == call.request.parameter_count() {
        // If there are no fields to extract, a user can also omit skip and max_results as we simply
        // output the count...
        (0, 0)
    } else {
        parse_limits(call, &mut parameter_index)?
    };

    let mut iter = table.table_scan().skip(skip as usize);

    emit_results(
        &call.request,
        &mut call.response,
        &table,
        parameter_index,
        &mut iter,
        limit as usize,
        I18nContext {
            primary_lang,
            fallback_lang,
            default_lang: table.default_lang_query(),
        },
    )?;

    Ok(())
}

/// For i18n queries this will read the `primary_lang` and `fallback_lang` parameter.
fn parse_langs(
    call: &Call,
    table: &Arc<Table>,
    translate: bool,
    parameter_index: &mut usize,
) -> anyhow::Result<(Option<Query>, Option<Query>)> {
    if translate {
        *parameter_index += 2;
        let primary_lang = call
            .request
            .str_parameter(*parameter_index - 2)
            .context("Missing primary language as parameter.")?;
        let fallback_lang = call
            .request
            .str_parameter(*parameter_index - 1)
            .context("Missing fallback language as parameter.")?;
        if primary_lang != fallback_lang {
            Ok((
                Some(table.compile(primary_lang)),
                Some(table.compile(fallback_lang)),
            ))
        } else {
            Ok((Some(table.compile(primary_lang)), None))
        }
    } else {
        Ok((None, None))
    }
}

/// Reads the `num_skip` and `max_values` parameter.
fn parse_limits(call: &Call, parameter_index: &mut usize) -> anyhow::Result<(i32, i32)> {
    *parameter_index += 2;

    Ok((
        call.request
            .int_parameter(*parameter_index - 2)
            .context("Missing or invalid skip parameter.")?,
        call.request
            .int_parameter(*parameter_index - 1)
            .context("Missing or invalid limit parameter.")?,
    ))
}

/// Emits the actual data for one or more matches.
fn emit_results<'a, I>(
    request: &Request,
    response: &mut Response,
    table: &Arc<Table>,
    parameter_index: usize,
    iter: &'a mut I,
    limit: usize,
    i18n: I18nContext,
) -> anyhow::Result<()>
where
    I: Iterator<Item = Element<'a>>,
{
    if request.parameter_count() == parameter_index {
        response.number(iter.count() as i64)?;
    } else {
        let mut results = Vec::new();
        let max_results = limit.min(MAX_RESULTS);
        for row in iter {
            if results.len() >= max_results {
                break;
            }

            results.push(row);
        }

        let queries = (parameter_index..request.parameter_count())
            .map(|index| table.compile(request.str_parameter(index).unwrap_or(".")))
            .collect::<Vec<Query>>();

        response.array(results.len() as i32)?;
        for row in results {
            response.array(request.parameter_count() as i32 - parameter_index as i32)?;
            for query in &queries {
                emit_element(query.execute(row), response, &i18n)?;
            }
        }
    }

    Ok(())
}

fn emit_element(
    element: Element,
    response: &mut Response,
    i18n: &I18nContext,
) -> anyhow::Result<()> {
    if let Some(string) = element.as_str() {
        response.bulk(string)?;
    } else if let Some(int) = element.as_int() {
        response.number(int)?;
    } else if let Some(bool) = element.try_as_bool() {
        response.boolean(bool)?;
    } else if element.is_list() {
        response.array(element.len() as i32)?;
        for child in element.iter() {
            emit_element(child, response, i18n)?;
        }
    } else if element.is_object() {
        if i18n.primary_lang.is_some() {
            if !emit_translated(element, i18n.primary_lang.as_ref(), response, i18n)?
                && !emit_translated(element, i18n.fallback_lang.as_ref(), response, i18n)?
                && !emit_translated(element, Some(i18n.default_lang), response, i18n)?
            {
                response.empty_string()?;
            }
        } else {
            response.bulk(to_json(element, i18n).to_string())?
        }
    } else {
        response.empty_string()?;
    }

    Ok(())
}

fn emit_translated(
    element: Element,
    lang: Option<&Query>,
    response: &mut Response,
    i18n: &I18nContext,
) -> anyhow::Result<bool> {
    if let Some(lang) = lang {
        let translated = lang.execute(element);
        if !translated.is_empty() {
            emit_element(translated, response, i18n)?;
            return Ok(true);
        }
    }

    Ok(false)
}

fn to_json(element: Element, i18n: &I18nContext) -> serde_json::value::Value {
    if let Some(string) = element.as_str() {
        serde_json::json!(string)
    } else if let Some(int) = element.as_int() {
        serde_json::json!(int)
    } else if let Some(bool) = element.try_as_bool() {
        serde_json::json!(bool)
    } else if element.is_list() {
        serde_json::json!(element
            .iter()
            .map(|child| to_json(child, i18n))
            .collect::<serde_json::value::Value>())
    } else if element.is_object() {
        hash_to_json(element, i18n)
    } else {
        serde_json::value::Value::Null
    }
}

fn hash_to_json(element: Element, i18n: &I18nContext) -> serde_json::value::Value {
    let mut hash = HashMap::new();
    for (key, child) in element.entries() {
        let _ = hash.insert(key, to_json(child, i18n));
    }

    serde_json::json!(hash)
}

/// For a set related call, we perform the lookup in the set dictionary while having
/// exclusive access to the underlying hash map. We then pass the **Arc** reference into
/// a separate thread so that multiple queries can be executed simultaneously.
async fn handle_set_call(mut call: Call, database: &HashMap<String, (Arc<Set>, String)>) {
    let set_name = if let Ok(name) = call.request.str_parameter(0) {
        name
    } else {
        call.complete(Err(CommandError::ClientError(anyhow::anyhow!(
            "Missing set name as first parameter!"
        ))));
        return;
    };

    let (set, _) = if let Some(set) = database.get(set_name) {
        set.clone()
    } else {
        let set_name = set_name.to_owned();
        call.complete(Err(CommandError::ClientError(anyhow::anyhow!(
            "Unknown set: {}",
            set_name
        ))));
        return;
    };

    let _ = tokio::spawn(async move {
        let token = call.token;
        match Commands::from_usize(token) {
            Some(Commands::Contains) => set_contains_command(&mut call, set).complete(call),
            Some(Commands::IndexOf) => set_index_of_command(&mut call, set).complete(call),
            _ => call.complete(Err(CommandError::ServerError(anyhow::anyhow!(
                "Unknown token received: {}!",
                token
            )))),
        }
    });
}

fn set_contains_command(call: &mut Call, set: Arc<Set>) -> CommandResult {
    if call.request.parameter_count() == 2 {
        call.response
            .boolean(set.contains(call.request.str_parameter(1)?))?;
    } else {
        call.response
            .array((call.request.parameter_count() - 1) as i32)?;
        for index in 1..call.request.parameter_count() {
            call.response
                .boolean(set.contains(call.request.str_parameter(index)?))?;
        }
    }

    Ok(())
}

fn set_index_of_command(call: &mut Call, set: Arc<Set>) -> CommandResult {
    if call.request.parameter_count() == 2 {
        call.response
            .number(set.index_of(call.request.str_parameter(1)?) as i64)?;
    } else {
        call.response
            .array((call.request.parameter_count() - 1) as i32)?;
        for index in 1..call.request.parameter_count() {
            call.response
                .number(set.index_of(call.request.str_parameter(index)?) as i64)?;
        }
    }

    Ok(())
}

/// Handles administrative commands while having exclusive access to the data structures
/// of the main actor.
fn handle_admin(
    command: DatabaseCommand,
    tables: &mut HashMap<String, Arc<Table>>,
    sets: &mut HashMap<String, (Arc<Set>, String)>,
) {
    match command {
        DatabaseCommand::CreateTable(name, table) => {
            log::info!(
                "New or updated table: {} ({} rows, {})",
                &name,
                table.len(),
                apollo_framework::fmt::format_size(table.allocated_memory())
            );
            let _ = tables.insert(name, Arc::new(table));
        }
        DatabaseCommand::DropTable(name) => {
            log::info!("Dropping table: {}...", &name);
            let _ = tables.remove(&name);
        }
        DatabaseCommand::CreateSet(source, name, set) => {
            log::info!(
                "New or updated set: {} ({} elements, {}, Source: {})",
                &name,
                set.len(),
                apollo_framework::fmt::format_size(set.allocated_memory()),
                source
            );
            let _ = sets.insert(name, (Arc::new(set), source));
        }
        DatabaseCommand::DropSets(source) => {
            log::info!("Dropping sets of: {}...", &source);
            sets.retain(|_, v| v.1 != source);
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::builder::Builder;
    use crate::idb::set::Set;
    use crate::idb::table::{IndexType, Table};
    use crate::idb::{install, Database, DatabaseCommand};
    use crate::ig::docs::Doc;
    use crate::ig::yaml::list_to_doc;
    use crate::server::{resp_protocol_loop, RespPayload};
    use crate::testing::{query_redis_async, test_async};
    use apollo_framework::config::Config;
    use apollo_framework::platform::Platform;
    use apollo_framework::server::Server;
    use std::sync::Arc;
    use tokio::time::Duration;
    use yaml_rust::YamlLoader;

    fn create_example_dataset() -> Doc {
        let input = r#"
code: "D"
iso:
  two: "de"
  three: "deu"
name:
  de: "Deutschland"
  en: "Germany"
---
code: "A"
iso:
  two: "at"
  three: "aut"
name:
  de: "Österreich"
  en: "Austria"
  xx: "Test"
---
code: "X"
name: Test
        "#;

        let rows = YamlLoader::load_from_str(input).unwrap();
        list_to_doc(rows.as_slice(), |_| true).unwrap()
    }

    #[test]
    fn integration_test_for_tables() {
        // We want exclusive access to both, the test-repo and the 1503 port on which we fire up
        // a test-server for our integration tests...
        log::info!("Acquiring shared resources...");
        let _guard = crate::testing::SHARED_TEST_RESOURCES.lock().unwrap();
        log::info!("Successfully acquired shared resources.");

        test_async(async {
            let (platform, database) = setup_environment().await;

            // Load an extremely elaborate example dataset...
            let dataset = create_example_dataset();
            let table = Table::new(
                dataset,
                vec![
                    IndexType::lookup("code"),
                    IndexType::lookup("iso.two"),
                    IndexType::fulltext("name"),
                ],
            )
            .unwrap();
            database
                .perform(DatabaseCommand::CreateTable("countries".to_string(), table))
                .await
                .unwrap();

            // Ensure that the command is processed...
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Expect that the table is present (a SCAN without fields to extract is a "count")...
            assert_eq!(
                query_redis_async(|con| redis::cmd("IDB.SCAN").arg("countries").query::<i32>(con))
                    .await
                    .unwrap(),
                3
            );

            // Perform some lookups...
            let result = query_redis_async(|con| {
                redis::cmd("IDB.LOOKUP")
                    .arg("countries")
                    .arg("code")
                    .arg("D")
                    .arg("iso.two")
                    .query::<Vec<Vec<String>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(result[0][0], "de");

            let result = query_redis_async(|con| {
                redis::cmd("IDB.LOOKUP")
                    .arg("countries")
                    .arg("code")
                    .arg("A")
                    .arg("iso.two")
                    .arg("iso.three")
                    .query::<Vec<Vec<(String, String)>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(result[0][0].0, "at");
            assert_eq!(result[0][0].1, "aut");

            // Ensure that querying "." returns the whole element as JSON...
            let result = query_redis_async(|con| {
                redis::cmd("IDB.LOOKUP")
                    .arg("countries")
                    .arg("code")
                    .arg("X")
                    .arg(".")
                    .query::<Vec<Vec<String>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(result[0][0], "{\"code\":\"X\",\"name\":\"Test\"}");

            // Ensure that querying an inner map returns the whole map as JSON (even in the
            // presence of xx fallback codes)...
            let result = query_redis_async(|con| {
                redis::cmd("IDB.LOOKUP")
                    .arg("countries")
                    .arg("code")
                    .arg("A")
                    .arg("name")
                    .query::<Vec<Vec<String>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(
                result[0][0],
                "{\"de\":\"Österreich\",\"en\":\"Austria\",\"xx\":\"Test\"}"
            );

            // Ensure that querying an inner map without a proper translation returns
            // an empty string (as no "xx" fallback code is present)..
            let result = query_redis_async(|con| {
                redis::cmd("IDB.ILOOKUP")
                    .arg("countries")
                    .arg("uu")
                    .arg("uu")
                    .arg("code")
                    .arg("D")
                    .arg("name")
                    .query::<Vec<Vec<String>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(result[0][0], "");

            // Ensure direct translations work...
            let result = query_redis_async(|con| {
                redis::cmd("IDB.ILOOKUP")
                    .arg("countries")
                    .arg("en")
                    .arg("de")
                    .arg("code")
                    .arg("A")
                    .arg("name")
                    .arg("name.de")
                    .query::<Vec<Vec<(String, String)>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(result[0][0].0, "Austria");
            assert_eq!(result[0][0].1, "Österreich");

            // Ensure fallbacks to the "xx" key work....
            let result = query_redis_async(|con| {
                redis::cmd("IDB.ILOOKUP")
                    .arg("countries")
                    .arg("it")
                    .arg("it")
                    .arg("code")
                    .arg("A")
                    .arg("name")
                    .arg("name.de")
                    .query::<Vec<Vec<(String, String)>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(result[0][0].0, "Test");
            assert_eq!(result[0][0].1, "Österreich");

            // Ensure searches work....
            let result = query_redis_async(|con| {
                redis::cmd("IDB.SEARCH")
                    .arg("countries")
                    .arg("0")
                    .arg("2")
                    .arg("*")
                    .arg("deutsch")
                    .arg("code")
                    .arg("name.de")
                    .query::<Vec<Vec<(String, String)>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(result[0][0].0, "D");
            assert_eq!(result[0][0].1, "Deutschland");

            database
                .perform(DatabaseCommand::DropTable("countries".to_string()))
                .await
                .unwrap();
            // Ensure that the command is processed...
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Expect an error as the table is gone now...
            assert_eq!(
                query_redis_async(|con| redis::cmd("IDB.SCAN")
                    .arg("countries")
                    .query::<Vec<i32>>(con))
                .await
                .is_none(),
                true
            );

            platform.terminate()
        });
    }

    async fn setup_environment() -> (Arc<Platform>, Arc<Database>) {
        //  Setup and create a platform...
        let platform = Builder::new().enable_all().build().await;
        install(platform.clone());

        // Specify a minimal config so that we run on a different port than a
        // production instance.
        platform
            .require::<Config>()
            .load_from_string(
                r#"
                     server:
                         port: 1503
                     "#,
                None,
            )
            .unwrap();

        // Fork the server in a separate thread..
        Server::fork_and_await(
            &platform.require::<Server<RespPayload>>(),
            &resp_protocol_loop,
        )
        .await;

        (platform.clone(), platform.require::<Database>())
    }

    #[test]
    fn integration_test_for_sets() {
        // We want exclusive access to both, the test-repo and the 1503 port on which we fire up
        // a test-server for our integration tests...
        log::info!("Acquiring shared resources...");
        let _guard = crate::testing::SHARED_TEST_RESOURCES.lock().unwrap();
        log::info!("Successfully acquired shared resources.");

        test_async(async {
            let (platform, database) = setup_environment().await;

            // Load an extremely elaborate example dataset...
            let mut set = Set::default();
            set.add("A".to_owned());
            set.add("B".to_owned());
            set.add("C".to_owned());

            database
                .perform(DatabaseCommand::CreateSet(
                    "test".to_string(),
                    "test_set".to_string(),
                    set,
                ))
                .await
                .unwrap();

            // Ensure that the command is processed...
            tokio::time::sleep(Duration::from_secs(2)).await;

            assert_eq!(
                query_redis_async(|con| redis::cmd("IDB.CONTAINS")
                    .arg("test_set")
                    .arg("A")
                    .query::<i32>(con))
                .await
                .unwrap(),
                1
            );
            assert_eq!(
                query_redis_async(|con| redis::cmd("IDB.CONTAINS")
                    .arg("test_set")
                    .arg("X")
                    .query::<i32>(con))
                .await
                .unwrap(),
                0
            );
            assert_eq!(
                query_redis_async(|con| redis::cmd("IDB.INDEX_OF")
                    .arg("test_set")
                    .arg("B")
                    .query::<i32>(con))
                .await
                .unwrap(),
                2
            );
            assert_eq!(
                query_redis_async(|con| redis::cmd("IDB.INDEX_OF")
                    .arg("test_set")
                    .arg("X")
                    .query::<i32>(con))
                .await
                .unwrap(),
                0
            );

            database
                .perform(DatabaseCommand::DropSets("test".to_string()))
                .await
                .unwrap();
            // Ensure that the command is processed...
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Expect an error as the set is gone now...
            assert_eq!(
                query_redis_async(|con| redis::cmd("IDB.CONTAINS")
                    .arg("test_set")
                    .arg("A")
                    .query::<Vec<i32>>(con))
                .await
                .is_none(),
                true
            );

            platform.terminate()
        });
    }
}
