# contractskit

US federal government contract awards per company for Rust.

```toml
[dependencies]
contractskit = "0.1.0"
```

```rust,no_run
#[tokio::main]
async fn main() -> contractskit::Result<()> {
    for a in contractskit::contracts_for("LMT").await?.iter().take(5) {
        println!("{} {} ${}", a.action_date, a.recipient_name, a.amount_usd);
    }
    Ok(())
}
```

Full documentation: <https://github.com/userFRM/contractskit>

Licensed under MIT OR Apache-2.0.
