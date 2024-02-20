use clap::Parser;
use color_eyre::Result;
use sqlx::postgres::PgConnectOptions;
use sqlx::{FromRow, PgPool};
use tabled::{Table, Tabled};
use time::macros::format_description;
use time::Date;
use tracing::info;
use tracing_subscriber::fmt::layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{registry, EnvFilter};

#[derive(Parser)]
pub struct Args {
    /// Naam van de database, normaalgesproken `koala`
    db_name: String,
    /// Naam van de gebruiker voor de database, noormaalgesproken `koala_manual`
    db_user: String,
    /// Wachtwoord van de gebruiker voor de database, vraag deze op bij de ITCrowd
    db_password: String,
    /// Datum van de start van het studiejaar, in het formaat `yyyy-mm-dd`
    study_year_start: String,
    /// Datum van de dag na de laatste NOVA activiteit, in het formaat `yyyy-mm-dd`
    date_after_nova: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    install_tracing();
    let args = Args::parse();

    info!(
        "'{}' v{} by '{}'",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        env!("CARGO_PKG_AUTHORS")
    );

    let driver = open_database(&args.db_name, &args.db_user, &args.db_password).await?;
    info!("Connected to database");

    let study_year_start = Date::parse(
        &args.study_year_start,
        format_description!("[year]-[month]-[day]"),
    )?;
    let date_after_nova = Date::parse(
        &args.date_after_nova,
        format_description!("[year]-[month]-[day]"),
    )?;

    info!("Collecting and printing metrics.");
    collect_and_print(&driver, study_year_start, date_after_nova).await?;

    info!("Done");
    Ok(())
}

#[derive(FromRow, Tabled)]
pub struct CodeCount {
    code: String,
    count: i64,
}

#[derive(FromRow, Tabled)]
pub struct JoinYearMembers {
    join_year: i32,
    members: i64,
}

#[derive(FromRow, Tabled)]
pub struct OnlyCount {
    count: i64,
}

async fn collect_and_print(
    driver: &PgPool,
    study_year_start: Date,
    date_after_nove: Date,
) -> Result<()> {
    let a2: Vec<CodeCount> = sqlx::query_as(
        "SELECT studies.code, COUNT(DISTINCT(members.id)) FROM members
                JOIN educations ON members.id = educations.member_id
                JOIN studies ON educations.study_id = studies.id
            WHERE educations.status = 0
            GROUP BY studies.code",
    )
    .fetch_all(&*driver)
    .await?;

    println!("A2 - Verdeling studies");
    println!("{}", Table::new(&a2).to_string());
    println!("Sum: {}", a2.iter().map(|x| x.count).sum::<i64>());
    println!();

    let a3: OnlyCount = sqlx::query_as(
        "SELECT COUNT(DISTINCT(members.id)) FROM members
                JOIN educations ON members.id = educations.member_id
                JOIN studies ON educations.study_id = studies.id
            WHERE educations.status = 0 AND members.join_date > $1",
    )
    .bind(&study_year_start)
    .fetch_one(&*driver)
    .await?;

    println!("A3 - Nieuwe leden");
    println!("{}", Table::new(vec![a3]).to_string());
    println!();

    let a4: OnlyCount = sqlx::query_as(
        "SELECT COUNT(DISTINCT(members.id)) FROM members
                INNER JOIN educations
                ON members.id = educations.member_id
            WHERE members.join_date > $1 AND educations.study_id < 5",
    )
    .bind(&study_year_start)
    .fetch_one(&*driver)
    .await?;

    println!("A4 - Nieuwe bachelor");
    println!("{}", Table::new(vec![a4]).to_string());
    println!();

    let a5: OnlyCount = sqlx::query_as(
        "SELECT COUNT(DISTINCT(members.id)) FROM members
                INNER JOIN educations
                ON members.id = educations.member_id
            WHERE members.join_date > $1 AND educations.study_id > 4",
    )
    .bind(&study_year_start)
    .fetch_one(&*driver)
    .await?;

    println!("A5 - Nieuew master");
    println!("{}", Table::new(vec![a5]).to_string());
    println!();

    let a6: Vec<CodeCount> = sqlx::query_as(
        "SELECT studies.code, COUNT(DISTINCT(members.id)) FROM members
                JOIN educations ON members.id = educations.member_id
                JOIN studies ON educations.study_id = studies.id
            WHERE educations.status = 0 AND members.join_date  > $1 group by studies.code",
    )
    .bind(&study_year_start)
    .fetch_all(&*driver)
    .await?;

    println!("A6 - Verdeling studies nieuwe leden");
    println!("{}", Table::new(&a6).to_string());
    println!("Sum: {}", a6.iter().map(|x| x.count).sum::<i64>());
    println!();

    let a7: OnlyCount = sqlx::query_as(
        "SELECT COUNT(DISTINCT(member_id)) FROM members INNER JOIN group_members
            ON members.id = group_members.member_id WHERE members.join_date > $1",
    )
    .bind(&study_year_start)
    .fetch_one(&*driver)
    .await?;

    println!("A7 - Nieuwe actieve leden");
    println!("{}", Table::new(vec![a7]).to_string());
    println!();

    let a8: Vec<JoinYearMembers> = sqlx::query_as(
        "SELECT
                EXTRACT(YEAR FROM generate_series)::int as join_year,count(distinct(members.id)) filter (
    	            where members.join_date > generate_series and members.join_date <= generate_series + interval '1 year'
	            ) as members
            FROM
                generate_series('2010-08-01'::date, $1::date, '1 year') as generate_series
            LEFT JOIN
                members ON members.join_date > generate_series AND members.join_date <= generate_series + interval '1 year'
            GROUP BY join_year;"
    )
        .bind(&study_year_start)
        .fetch_all(&*driver)
        .await?;

    println!("A8 - Nieuwe leden sinds 2010");
    println!("{}", Table::new(&a8).to_string());
    println!();

    let a11: Vec<CodeCount> = sqlx::query_as(
        "SELECT studies.code , COUNT(DISTINCT(members.id)) FROM members
                inner join group_members ON members.id = group_members.member_id
                JOIN educations on members.id = educations.member_id
                join studies on educations.study_id = studies.id
            WHERE educations.status = 0
                AND members.join_date  > $1
            group by studies.code",
    )
    .bind(&study_year_start)
    .fetch_all(&*driver)
    .await?;

    println!("A11 - Verdeling nieuwe actieve leden");
    println!("{}", Table::new(&a11).to_string());
    println!(
        "Sum: {} (Kan anders zijn dan het getal van A7, i.v.m dubbele studies)",
        a11.iter().map(|x| x.count).sum::<i64>()
    );
    println!();

    let a12: OnlyCount = sqlx::query_as(
        "SELECT COUNT(DISTINCT(lid_id)) FROM(SELECT DISTINCT(members.id) as lid_id,
                members.first_name, members.last_name, participants.activity_id
                FROM members INNER JOIN participants ON members.id = participants.member_id
            WHERE members.join_date > $1) AS dinges INNER JOIN activities on
                dinges.activity_id = activities.id WHERE activities.start_date > $2",
    )
    .bind(&study_year_start)
    .bind(&date_after_nove)
    .fetch_one(&*driver)
    .await?;

    println!("A12 - Sjaars bij activiteiten");
    println!("{}", Table::new(vec![a12]).to_string());
    println!();

    #[derive(FromRow)]
    struct IdName {
        id: i32,
        name: String,
    }

    let extern_activities = sqlx::query_as("SELECT id,name FROM activities WHERE start_date > $1")
        .bind(&study_year_start)
        .fetch_all(&*driver)
        .await?
        .into_iter()
        .filter(|act: &IdName| act.name.to_lowercase().trim().starts_with("extern"))
        .map(|act| act.id)
        .collect::<Vec<_>>();

    let a13: OnlyCount = sqlx::query_as(
        "SELECT COUNT(DISTINCT(lid_id)) FROM(SELECT DISTINCT(members.id) as lid_id,
            participants.activity_id FROM
            members INNER JOIN participants ON members.id = participants.member_id)
            AS dinges INNER JOIN activities ON dinges.activity_id = activities.id WHERE activities.id IN (SELECT unnest($1::integer[]))"
    )
        .bind(extern_activities)
        .fetch_one(&*driver)
        .await?;

    println!("A13 - Leden bij Extern activiteiten");
    println!("{}", Table::new(vec![a13]).to_string());
    println!();

    println!("Done. Heel veel success met de ALV ♡");

    Ok(())
}

async fn open_database(db_name: &str, user: &str, passw: &str) -> Result<PgPool> {
    let opts = PgConnectOptions::new()
        .host("127.0.0.1")
        .database(db_name)
        .username(user)
        .password(passw);

    Ok(PgPool::connect_with(opts).await?)
}

fn install_tracing() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "INFO");
    }

    registry()
        .with(EnvFilter::from_default_env())
        .with(layer().compact())
        .init();
}
