const Wallet = require("ethereumjs-wallet").default;
const solanaWeb3 = require("@solana/web3.js");
const Arweave = require("arweave");
const fs = require("fs");

// Generate a new ETH wallet
const ethWallet = Wallet.generate();

// Generate a new Solana wallet
const solWallet = solanaWeb3.Keypair.generate();

//Initialize Arweave
const arweave = Arweave.init({});

// We get the address, public key and private key
const address = ethWallet.getAddressString();
const publicKey = ethWallet.getPublicKeyString();
const privateKey = ethWallet.getPrivateKeyString().substring(2);

console.log("Ethereum Info: ");
console.log("Wallet address:", address);
console.log("Public key:", publicKey);
console.log("Private key:", privateKey);
console.log("");

solSecretKey = "[" + solWallet.secretKey.toString() + "]";
// Log the wallet address, public key, and private key to the console
console.log("Solana Info: ");
console.log("Wallet address:", solWallet.publicKey.toBase58());
console.log("Public key:", solWallet.publicKey.toString());
console.log("Private key:", solSecretKey);

arweave.wallets.generate().then((key) => {
  // Save the private key in a file
  fs.writeFile("arweave.json", JSON.stringify(key), function (err) {
    if (err) {
      return console.log(err);
    }
    console.log("\nPrivate key saved to arweave.json");
  });
  //Log wallet address
  arweave.wallets.jwkToAddress(key).then((address) => {
    console.log("\nArweave address:", address);
  });
});

// Save the private key in a file
fs.writeFile("eth.pk", privateKey, function (err) {
  if (err) {
    return console.log(err);
  }
  console.log("\nPrivate key saved to eth.pk");
});

// Save the private key to a file in the same directory as the script
fs.writeFile("solana.pk", solSecretKey, function (err) {
  if (err) {
    console.log(err);
  } else {
    console.log("\nPrivate key saved to solana.pk");
  }
});
