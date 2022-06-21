//! # sqlx-plus
//!
//! Please refer [README](https://github.com/sifyfy/sqlx-plus).
//!

use async_trait::async_trait;
use itertools::Itertools;
use sqlx::{database::HasArguments, Executor, IntoArguments};

pub use sqlx_plus_macros::Insertable;

pub trait QueryBindExt<'q, DB: sqlx::Database>: Sized {
    fn bind<T>(self, value: T) -> Self
    where
        T: 'q + Send + sqlx::Encode<'q, DB> + sqlx::Type<DB>;

    fn bind_with<T>(self, value: T, bind_fn: impl Fn(Self, T) -> Self) -> Self {
        bind_fn(self, value)
    }

    fn bind_multi<T>(self, values: impl IntoIterator<Item = T>) -> Self
    where
        T: 'q + Send + sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        values.into_iter().fold(self, |q, v| q.bind(v))
    }

    fn bind_multi_with<T: 'q>(
        self,
        values: impl IntoIterator<Item = &'q T>,
        bind_fn: impl Fn(Self, &'q T) -> Self,
    ) -> Self {
        values.into_iter().fold(self, |q, x| bind_fn(q, x))
    }

    fn bind_fields<T: Insertable<Database = DB>>(self, value: &'q T) -> Self {
        value.bind_fields(self)
    }

    fn bind_multi_fields<T: Insertable<Database = DB> + 'q>(
        self,
        values: impl IntoIterator<Item = &'q T>,
    ) -> Self {
        self.bind_multi_with(values, |q, v| q.bind_fields(v))
    }
}

impl<'q, DB: sqlx::Database> QueryBindExt<'q, DB>
    for sqlx::query::Query<'q, DB, <DB as HasArguments<'q>>::Arguments>
{
    fn bind<T>(self, value: T) -> Self
    where
        T: 'q + Send + sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        sqlx::query::Query::bind(self, value)
    }
}

impl<'q, DB, O> QueryBindExt<'q, DB>
    for sqlx::query::QueryAs<'q, DB, O, <DB as HasArguments<'q>>::Arguments>
where
    DB: sqlx::Database,
{
    fn bind<T>(self, value: T) -> Self
    where
        T: 'q + Send + sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        sqlx::query::QueryAs::bind(self, value)
    }
}

impl<'q, DB, O> QueryBindExt<'q, DB>
    for sqlx::query::QueryScalar<'q, DB, O, <DB as HasArguments<'q>>::Arguments>
where
    DB: sqlx::Database,
{
    fn bind<T>(self, value: T) -> Self
    where
        T: 'q + Send + sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        sqlx::query::QueryScalar::bind(self, value)
    }
}

pub trait Insertable: Sized {
    type Database: sqlx::Database;

    fn table_name() -> &'static str;

    fn insert_columns() -> Vec<&'static str>;

    fn bind_fields<'q, Q>(&'q self, q: Q) -> Q
    where
        Q: QueryBindExt<'q, Self::Database>;
}

impl<T: Insertable + Sync> Insertable for &T {
    type Database = T::Database;

    fn table_name() -> &'static str {
        T::table_name()
    }

    fn insert_columns() -> Vec<&'static str> {
        T::insert_columns()
    }

    fn bind_fields<'q, Q>(&'q self, q: Q) -> Q
    where
        Q: QueryBindExt<'q, Self::Database>,
    {
        (*self).bind_fields(q)
    }
}

#[async_trait]
pub trait Inserter<DB: sqlx::Database>: Sized {
    async fn insert<T>(self, value: &T) -> anyhow::Result<DB::QueryResult>
    where
        T: Insertable<Database = DB> + Sync;

    async fn bulk_insert_with_table_name_and_chunk_size<T>(
        self,
        table_name: &str,
        chunk_size: usize,
        values: &[T],
    ) -> anyhow::Result<Vec<DB::QueryResult>>
    where
        T: Insertable<Database = DB> + Sync;

    async fn bulk_insert<T>(self, values: &[T]) -> anyhow::Result<Vec<DB::QueryResult>>
    where
        T: Insertable<Database = DB> + Sync,
    {
        self.bulk_insert_with_table_name(T::table_name(), values)
            .await
    }

    async fn bulk_insert_with_table_name<T>(
        self,
        table_name: &str,
        values: &[T],
    ) -> anyhow::Result<Vec<DB::QueryResult>>
    where
        T: Insertable<Database = DB> + Sync,
    {
        self.bulk_insert_with_table_name_and_chunk_size(
            table_name,
            30000 / T::insert_columns().len(),
            values,
        )
        .await
    }

    async fn bulk_insert_with_chunk_size<T>(
        self,
        chunk_size: usize,
        values: &[T],
    ) -> anyhow::Result<Vec<DB::QueryResult>>
    where
        T: Insertable<Database = DB> + Sync,
    {
        self.bulk_insert_with_table_name_and_chunk_size(T::table_name(), chunk_size, values)
            .await
    }
}

macro_rules! impl_inserter {
    ( $db:ty ) => {
        #[async_trait]
        impl<E> Inserter<$db> for &'_ mut E
        where
            E: Send,
            for<'a> &'a mut E: Executor<'a, Database = $db>,
        {
            async fn insert<T>(
                self,
                value: &T,
            ) -> anyhow::Result<<$db as sqlx::Database>::QueryResult>
            where
                T: Insertable<Database = $db> + Sync,
            {
                Ok(insert(self, value).await?)
            }

            async fn bulk_insert_with_table_name_and_chunk_size<T>(
                self,
                table_name: &str,
                chunk_size: usize,
                values: &[T],
            ) -> anyhow::Result<Vec<<$db as sqlx::Database>::QueryResult>>
            where
                T: Insertable<Database = $db> + Sync,
            {
                Ok(
                    bulk_insert_with_table_name_and_chunk_size(
                        self, table_name, chunk_size, values,
                    )
                    .await?,
                )
            }
        }

        #[async_trait]
        impl Inserter<$db> for &'_ sqlx::Pool<$db> {
            async fn insert<T>(
                self,
                value: &T,
            ) -> anyhow::Result<<$db as sqlx::Database>::QueryResult>
            where
                T: Insertable<Database = $db> + Sync,
            {
                Ok(self.acquire().await?.insert(value).await?)
            }

            async fn bulk_insert_with_table_name_and_chunk_size<T>(
                self,
                table_name: &str,
                chunk_size: usize,
                values: &[T],
            ) -> anyhow::Result<Vec<<$db as sqlx::Database>::QueryResult>>
            where
                T: Insertable<Database = $db> + Sync,
            {
                Ok(self
                    .acquire()
                    .await?
                    .bulk_insert_with_table_name_and_chunk_size(table_name, chunk_size, values)
                    .await?)
            }
        }
    };
}

#[cfg(feature = "sqlite")]
impl_inserter!(sqlx::Sqlite);
#[cfg(feature = "mysql")]
impl_inserter!(sqlx::MySql);
#[cfg(feature = "postgres")]
impl_inserter!(sqlx::Postgres);
#[cfg(feature = "mssql")]
impl_inserter!(sqlx::Mssql);

pub trait PlaceHolders: sqlx::Database {
    /// `start_num` is for only PostgreSQL, it is ignored in other RDB.
    #[allow(unused_variables)]
    fn placeholders(num: usize, start_num: Option<usize>) -> String {
        placeholders(num)
    }

    /// `start_num` is for only PostgreSQL, it is ignored in other RDB.
    #[allow(unused_variables)]
    fn placeholders_for_bulk_insert_values<I, T>(values: I, start_num: Option<usize>) -> String
    where
        I: Iterator<Item = T>,
        T: Insertable<Database = Self>,
    {
        placeholders_for_bulk_insert_values(values)
    }
}

#[cfg(feature = "sqlite")]
impl PlaceHolders for sqlx::Sqlite {}

#[cfg(feature = "mysql")]
impl PlaceHolders for sqlx::MySql {}

#[cfg(feature = "mssql")]
impl PlaceHolders for sqlx::Mssql {}

#[cfg(feature = "postgres")]
impl PlaceHolders for sqlx::Postgres {
    fn placeholders(num: usize, start_num: Option<usize>) -> String {
        placeholders_postgres(num, start_num)
    }

    fn placeholders_for_bulk_insert_values<I, T>(values: I, start_num: Option<usize>) -> String
    where
        I: Iterator<Item = T>,
        T: Insertable<Database = Self>,
    {
        placeholders_for_bulk_insert_values_postgres(values, start_num)
    }
}

/// Generate placeholders string like `?, ?, ..., ?`.
pub fn placeholders(num: usize) -> String {
    (0..num).map(|_| "?").join(",")
}

/// Generate placeholders string like `(?, ?, ..., ?), (?, ?, ..., ?), ..., (?, ?, ..., ?)`.
pub fn placeholders_for_bulk_insert_values<I, T>(values: I) -> String
where
    I: Iterator<Item = T>,
    T: Insertable,
{
    format!(
        "({})",
        values
            .map(|_| placeholders(T::insert_columns().len()))
            .join("),(")
    )
}

/// Generate placeholders string like `$1, $2, ..., $n`.
pub fn placeholders_postgres(num: usize, start_num: Option<usize>) -> String {
    let start_num = start_num.unwrap_or(1);

    if usize::MAX - start_num < num {
        panic!("num > usize::MAX - start_num");
    }

    (0..num)
        .zip(start_num..(start_num + num))
        .map(|(_, i)| format!("${}", i))
        .join(",")
}

/// Generate placeholders string like `($1, $2, ..., $n), ($o, $p, ..., $q), ..., ($r, $s, ..., $u)`.
pub fn placeholders_for_bulk_insert_values_postgres<'a, I, T>(
    values: I,
    start_num: Option<usize>,
) -> String
where
    I: Iterator<Item = T>,
    T: Insertable,
{
    let start_num = start_num.unwrap_or(1);

    format!(
        "({})",
        values
            .enumerate()
            .map(|(i, _)| {
                let num_of_fields = T::insert_columns().len();
                let start_num = start_num + i * num_of_fields;
                placeholders_postgres(num_of_fields, Some(start_num))
            })
            .join("),(")
    )
}

async fn insert<T, E, DB>(executor: &mut E, value: &T) -> anyhow::Result<DB::QueryResult>
where
    DB: sqlx::Database + PlaceHolders,
    T: Insertable<Database = DB> + Sync,
    for<'e> &'e mut E: Executor<'e, Database = DB>,
    for<'q> <DB as HasArguments<'q>>::Arguments: IntoArguments<'q, DB>,
{
    let sql = format!(
        r#"
            INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
        "#,
        table_name = T::table_name(),
        columns = T::insert_columns().join(","),
        placeholders = DB::placeholders(T::insert_columns().len(), None),
    );

    sqlx::query(&sql)
        .bind_fields(value)
        .execute(executor)
        .await
        .map_err(From::from)
}

async fn bulk_insert_with_table_name_and_chunk_size<T, E, DB>(
    executor: &mut E,
    table_name: &str,
    chunk_size: usize,
    values: &[T],
) -> anyhow::Result<Vec<DB::QueryResult>>
where
    DB: sqlx::Database + PlaceHolders,
    T: Insertable<Database = DB> + Sync,
    for<'e> &'e mut E: Executor<'e, Database = DB>,
    for<'q> <DB as HasArguments<'q>>::Arguments: IntoArguments<'q, DB>,
{
    let mut results = Vec::with_capacity(values.len() / chunk_size);

    for chunk in values.chunks(chunk_size) {
        let sql = format!(
            r#"
                    INSERT INTO {table_name} ({columns}) VALUES {placeholders}
            "#,
            columns = T::insert_columns().join(","),
            placeholders = DB::placeholders_for_bulk_insert_values(chunk.iter(), None),
        );
        let result = sqlx::query(&sql)
            .bind_multi_fields(chunk)
            .execute(&mut *executor)
            .await?;

        results.push(result);
    }

    Ok(results)
}
