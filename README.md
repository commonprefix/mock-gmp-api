# mock-gmp-api
A mock implementation of some basic components of Axelar's GMP API.  
The goal is to set up a local testing environment for the different relayers used in the chain integrations.

## Usage:

After launching PostgreSQL, run:  
 `sqlx migrate run --database-url postgres://postgres:postgres@localhost:5432/mock-gmp-api`

To run the server : `cargo run --bin server`  
To run the client : `cargo run --bin client`

## Server Endpoints:  

GET /chain/\<chain_name\>/tasks  
GET /contracts/\<contract_address\>/broadcasts/\<broadcast_id\>  
GET /payloads/0x\<hash\>
POST /chain/\<chain_name\>/task   
POST /chain/\<chain_name\>/events    
POST /contracts/\<contract_address\>/broadacasts
POST /contracts/\<contract_address\>/queries
GET /contracts/\<contract_address\>/broadcasts/\<query_id\>  
POST /payloads