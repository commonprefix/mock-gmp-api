# mock-gmp-api
A mock implementation of some basic components of Axelar's GMP API
The goal is to set up a local testing environment for the different relayers used in the chain integrations.

## Usage:

After launching PostgreSQL, run:  
 `sqlx migrate run --database-url postgres://postgres:postgres@localhost:5432/mock-gmp-api`

To run the server : `cargo run --bin server`  
To run the client : `cargo run --bin client`

## Server Endpoints:

GET /tasks  
POST /events  
POST /\<address\>/broadacast  