use std::borrow::Cow;

use async_trait::async_trait;
use sqlx::prelude::*;

#[tokio::test]
async fn test_main() -> anyhow::Result<()> {
    let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:").await?;

    {
        let mut conn = pool.acquire().await?;
        let mut tx = conn.begin().await?;

        tx.setup_tables().await?;
        tx.setup_user().await?;
        tx.commit().await?;
    }

    {
        let mut conn = pool.acquire().await?;
        let mut tx = conn.begin().await?;

        assert_eq!(
            tx.get_user_by_name_and_password("xxxSHINICHIxxx", "password3")
                .await?,
            Some(User {
                id: 3,
                name: "xxxSHINICHIxxx".into(),
                password: "password3".into()
            })
        );

        assert_eq!(
            tx.get_user_by_name_and_password("hoge", "password4")
                .await?,
            Some(User {
                id: 4,
                name: "hoge".into(),
                password: "password4".into(),
            })
        );

        tx.commit().await?;
    }

    Ok(())
}

type Database = sqlx::Sqlite;

#[async_trait]
trait SetupDatabase {
    async fn setup_tables(&mut self) -> anyhow::Result<()>;
}

#[async_trait]
impl SetupDatabase for sqlx::Transaction<'_, Database> {
    async fn setup_tables(&mut self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE user (
                    id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    name        TEXT NOT NULL UNIQUE,
                    password    TEXT NOT NULL
                );
            "#,
        )
        .execute(self)
        .await?;

        Ok(())
    }
}

#[async_trait]
trait SetupUser {
    async fn setup_user(&mut self) -> anyhow::Result<()>;
}

#[async_trait]
impl<T> SetupUser for T
where
    T: Send,
    for<'e> &'e mut T: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    async fn setup_user(&mut self) -> anyhow::Result<()> {
        use sqlx_plus::QueryBindExt;

        sqlx_plus::bulk_insert(
            &[
                UserInsert {
                    name: Cow::from("aaabbb"),
                    password: Cow::from("password1"),
                },
                UserInsert {
                    name: Cow::from("heyheyhey"),
                    password: Cow::from("password2"),
                },
                UserInsert {
                    name: Cow::from("xxxSHINICHIxxx"),
                    password: Cow::from("password3"),
                },
            ],
            self,
        )
        .await?;

        sqlx_plus::insert(
            &UserInsert {
                name: Cow::from("hoge"),
                password: Cow::from("password4"),
            },
            self,
        )
        .await?;

        let user = "fuga";
        let password = "password5";
        sqlx::query(r#"INSERT INTO user (name, password) VALUES (?, ?)"#)
            .bind_multi(&["fuga", "password5"])
            .execute(self)
            .await?;

        Ok(())
    }
}

#[async_trait]
trait UserRepository {
    async fn get_user_by_name_and_password(
        &mut self,
        name: &str,
        password: &str,
    ) -> Result<Option<User>, anyhow::Error>;
}

#[async_trait]
impl<T> UserRepository for T
where
    for<'e> &'e mut T: Executor<'e, Database = sqlx::Sqlite>,
{
    async fn get_user_by_name_and_password(
        &mut self,
        name: &str,
        password: &str,
    ) -> Result<Option<User>, anyhow::Error> {
        use sqlx_plus::QueryBindExt;

        sqlx::query_as("SELECT * FROM user WHERE name = ? AND password = ?")
            .bind_multi(&[name, password])
            .fetch_optional(self)
            .await
            .map_err(From::from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
struct UserName(String);

impl From<&str> for UserName {
    fn from(s: &str) -> Self {
        UserName(s.into())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, sqlx::FromRow)]
struct User {
    id: i64,
    name: UserName,
    password: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, sqlx_plus::Insertable)]
#[insertable(sqlx::Sqlite, "user")]
struct UserInsert<'a> {
    name: Cow<'a, str>,
    password: Cow<'a, str>,
}
