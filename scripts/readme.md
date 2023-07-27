# Testing Scripts

## Key Generation

This script generates Ethereum, Solana, and Arweave wallets, including their wallet addresses, public keys, and private keys.

It also saves the private keys to individual files for each wallet type.

### Dependencies

To run this script, you will need the following packages:

ethereumjs-wallet
@solana/web3.js
arweave

Install required packages using the following command:

`npm install ethereumjs-wallet @solana/web3.js arweave`

### Usage

To generate wallets and keys, simply run:

`node key-generation.cjs`

The script will output the wallet address for Ethereum, Solana, and Arweave and also the public and private key for Ethereum and Solana

It will also save the private keys in the following files in the same directory as the script:

Ethereum private key: eth.pk

Solana private key: solana.pk

Arweave private key: arweave.json

Custom data payloads are currently limited to ~500MiB as we re not using streams.

## Data Item generation

This script generates and signs data items using the Arweave, Ethereum, and Solana signatures.
The signed data items are then saved as separate files.

### Useful values

Default tags:

`{ name: "Content-Type", value: "text/plain" }`

Empty data items (only signatures + default tags):

Arweave: 1.070 bytes

ETH: 176 bytes

SOL: 142 bytes

### Dependencies

To run this script, you will need to have the following packages:

arbundles
bs58

Install them using:

`npm install arbundles bs58`

### Usage

Setting the following environment variables before running the script is optional:

DATA*ITEM_TARGET(OPTIONAL): The target ID for the data item (43 characters from the set [a-zA-Z0-9*-]).

DATA_ITEM_ANCHOR(OPTIONAL): The anchor string for the data item (no more than 32 bytes when UTF-8 encoded).

DATA_PAYLOAD_SIZE(OPTIONAL): Custom size for data item payload, a number.

VALID_DATA_ITEMS(OPTIONAL): Providing any value of 0 (zero) will generate invalid data items.

CUSTOM_FILE(OPTIONAL): Provide a relative path to a file inside double quotes.

OUTFILE_NAME: The base name for the output files.

Prepare wallet files for Arweave, Ethereum, and Solana. These files must contain the private keys:

arweave.json: The Arweave wallet file in JSON format.
eth.pk: The Ethereum wallet private key file.
solana.pk: The Solana wallet private key file in JSON format.

Use `key-generation.cjs` for this

Run:

`node generate-data-items-all.cjs`

The script will generate and sign data items for Arweave, Ethereum, and Solana.

Signed data items will be saved as separate files using the provided base name and the chain abbreviation (AR, ETH, SOL) as follows:

[OUTFILE_NAME]\_AR

[OUTFILE_NAME]\_ETH

[OUTFILE_NAME]\_SOL
