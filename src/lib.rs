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
    for sqlx::query::Query<'q, DB, <DB as sqlx::database::HasArguments<'q>>::Arguments>
{
    fn bind<T>(self, value: T) -> Self
    where
        T: 'q + Send + sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        sqlx::query::Query::bind(self, value)
    }
}

impl<'q, DB, O> QueryBindExt<'q, DB>
    for sqlx::query::QueryAs<'q, DB, O, <DB as sqlx::database::HasArguments<'q>>::Arguments>
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
    for sqlx::query::QueryScalar<'q, DB, O, <DB as sqlx::database::HasArguments<'q>>::Arguments>
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

pub trait Insertable {
    type Database: sqlx::Database;

    fn table_name() -> &'static str;

    fn insert_columns() -> Vec<&'static str>;

    fn bind_fields<'q, Q>(&'q self, q: Q) -> Q
    where
        Q: QueryBindExt<'q, Self::Database>;
}

impl<T: Insertable> Insertable for &T {
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

/// Generate placeholders string like `?, ?, ..., ?`.
pub fn placeholders(num: usize) -> String {
    (0..num).map(|_| "?").join(",")
}

/// Generate placeholders string like `(?, ?, ..., ?), (?, ?, ..., ?), ..., (?, ?, ..., ?)`.
pub fn bulk_insert_values_placeholders<'a, I, T>(values: I) -> String
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

pub async fn insert<T, E, DB>(value: &T, executor: &mut E) -> anyhow::Result<DB::QueryResult>
where
    DB: sqlx::Database,
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
        placeholders = placeholders(T::insert_columns().len()),
    );

    sqlx::query(&sql)
        .bind_fields(value)
        .execute(executor)
        .await
        .map_err(From::from)
}

pub async fn bulk_insert_with_table_name_and_chunk_size<T, E, DB>(
    values: &[T],
    table_name: &str,
    chunk_size: usize,
    executor: &mut E,
) -> anyhow::Result<Vec<DB::QueryResult>>
where
    DB: sqlx::Database,
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
            placeholders = bulk_insert_values_placeholders(chunk.iter()),
        );
        let result = sqlx::query(&sql)
            .bind_multi_fields(chunk)
            .execute(&mut *executor)
            .await?;

        results.push(result);
    }

    Ok(results)
}

pub async fn bulk_insert<T, E, DB>(
    values: &[T],
    executor: &mut E,
) -> anyhow::Result<Vec<DB::QueryResult>>
where
    DB: sqlx::Database,
    T: Insertable<Database = DB> + Sync,
    for<'e> &'e mut E: Executor<'e, Database = DB>,
    for<'q> <DB as HasArguments<'q>>::Arguments: IntoArguments<'q, DB>,
{
    bulk_insert_with_table_name(values, T::table_name(), executor).await
}

pub async fn bulk_insert_with_chunk_size<T, E, DB>(
    values: &[T],
    chunk_size: usize,
    executor: &mut E,
) -> anyhow::Result<Vec<DB::QueryResult>>
where
    DB: sqlx::Database,
    T: Insertable<Database = DB> + Sync,
    for<'e> &'e mut E: Executor<'e, Database = DB>,
    for<'q> <DB as HasArguments<'q>>::Arguments: IntoArguments<'q, DB>,
{
    bulk_insert_with_table_name_and_chunk_size(values, T::table_name(), chunk_size, executor).await
}

pub async fn bulk_insert_with_table_name<T, E, DB>(
    values: &[T],
    table_name: &str,
    executor: &mut E,
) -> anyhow::Result<Vec<DB::QueryResult>>
where
    DB: sqlx::Database,
    T: Insertable<Database = DB> + Sync,
    for<'e> &'e mut E: Executor<'e, Database = DB>,
    for<'q> <DB as HasArguments<'q>>::Arguments: IntoArguments<'q, DB>,
{
    bulk_insert_with_table_name_and_chunk_size(
        values,
        table_name,
        30000 / T::insert_columns().len(),
        executor,
    )
    .await
}
