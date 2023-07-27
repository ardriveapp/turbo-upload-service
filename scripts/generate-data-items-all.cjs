const { createData } = require("arbundles/src/ar-data-create");
const ArweaveSigner =
  require("arbundles/src/signing/chains/ArweaveSigner").default;
//Be aware this signer is lower camel case, not Upper camel case
const EthereumSigner =
  require("arbundles/src/signing/chains/ethereumSigner").default;
const SolanaSigner =
  require("arbundles/src/signing/chains/SolanaSigner").default;
const fs = require("fs");
const bs58 = require("bs58");
const path = require("path");

//Folder where we store out dataItems
let folderName = "customDataItems";

if (!fs.existsSync(folderName)) {
  fs.mkdirSync(folderName);
}

(async () => {
  const target = process.env.DATA_ITEM_TARGET;
  const anchor = process.env.DATA_ITEM_ANCHOR;
  const customFile = process.env.CUSTOM_FILE || "";
  const valid = process.env.VALID_DATA_ITEMS || 1;
  const sizeRaw = process.env.DATA_PAYLOAD_SIZE || -1;
  let size = Number(sizeRaw);

  const arweaveWalletFile = "arweave.json";
  const ethereumWalletFile = "eth.pk";
  const solanaWalletFile = "solana.pk";

  const dataItemCreateOpts = {
    target,
    anchor,
    tags: [{ name: "Content-Type", value: "text/plain" }],
  };

  //const walletType = process.env.PRIV_KEY_TYPE ?? 1; /*Arweave*/
  const outfileName = process.env.OUTFILE_NAME;

  if (target && !target.match(/^[a-zA-Z0-9_-]{43}$/)) {
    console.error(
      "ERROR: DATA_ITEM_TARGET, when provided, must be 43 characters from the set [a-zA-Z0-9_-] "
    );
    process.exit(1);
  }

  if (anchor && Buffer.byteLength(anchor, "utf8") > 32) {
    console.error(
      "ERROR: DATA_ITEM_ANCHOR, when provided, must be a string no greater than 32 bytes when UTF-8 encoded"
    );
    process.exit(1);
  }

  if (!arweaveWalletFile) {
    console.error("ERROR: Must provide a PRIV_KEY_FILE_ARWEAVE value!");
    process.exit(1);
  }

  if (!ethereumWalletFile) {
    console.error("ERROR: Must provide a PRIV_KEY_FILE_ETH value!");
    process.exit(1);
  }

  if (!solanaWalletFile) {
    console.error("ERROR: Must provide a PRIV_KEY_FILE_SOL value!");
    process.exit(1);
  }

  if (!outfileName) {
    console.error("ERROR: Must provide an OUTFILE_NAME value!");
    process.exit(1);
  }

  //Read wallet files
  const keyFileBufferArweave = fs.readFileSync(arweaveWalletFile, "utf-8");
  const keyFileBufferEthereum = fs.readFileSync(ethereumWalletFile, "utf-8");
  const keyFileBufferSolana = fs.readFileSync(solanaWalletFile, "utf-8");

  function solanaSignFetch() {
    //Sign solana
    // Read and parse the Solana key file
    const keyFileDataSol = JSON.parse(keyFileBufferSolana);
    const secretKey = Uint8Array.from(keyFileDataSol);

    // Convert the secret key to base58 format
    const secretKeyBase58 = bs58.encode(Buffer.from(secretKey));
    let signerSol = new SolanaSigner(secretKeyBase58);
    return signerSol;
  }

  function arweaveSignFetch() {
    //Sign arweave
    const keyFileDataAr = JSON.parse(keyFileBufferArweave);
    let signerAr = new ArweaveSigner(keyFileDataAr);
    return signerAr;
  }

  function getSignature(dataItemSignerType) {
    switch (dataItemSignerType) {
      case "Arweave": {
        return arweaveSignFetch();
      }
      case "Ethereum": {
        return new EthereumSigner(keyFileBufferEthereum.toString().trim());
      }
      case "Solana": {
        return solanaSignFetch();
      }
    }
  }

  function customData(size) {
    if (fs.existsSync(customFile)) {
      data = fs.readFileSync(customFile, "utf-8");
      return data;
    } else {
      if (customFile != "") {
        console.error("ERROR: Cannot resolve CUSTOM_FILE value!");
      }
    }
    if (size < 0) {
      data = "hello";
      return data;
    } else {
      sizedBuffer = Buffer.alloc(size, 1);
      const data = sizedBuffer.toString();
      return data;
    }
  }

  async function signDataItem(dataItemSignerType, size) {
    let signer = getSignature(dataItemSignerType);
    let data = customData(size);
    console.log("Data payload size: " + Buffer.byteLength(data));
    const d = createData(data, signer, dataItemCreateOpts);
    await d.sign(signer);
    const withTargetStr = target ? `with target '${target}' ` : "";
    const withAnchorStr = anchor
      ? `${target ? "and" : "with"} anchor '${anchor} ' `
      : "";
    let fileName = outfileName + "_" + dataItemSignerType;
    let folderPath = path.join(folderName, fileName);
    if (valid == 0) {
      console.log("Writing invalid data items");
      let invalidData = Buffer.from("broken" + d.getRaw());
      fs.writeFileSync(folderPath, invalidData);
    } else {
      console.log(
        `Writing signed data items ${withTargetStr}${withAnchorStr}to file ${outfileName}_${dataItemSignerType}...`
      );
      if (size != -1 || fs.existsSync(customData)) {
        console.log("Total File Size: " + Buffer.byteLength(d.getRaw()));
      }
      fs.writeFileSync(folderPath, d.getRaw());
    }
  }

  signDataItem("Arweave", size);
  signDataItem("Ethereum", size);
  signDataItem("Solana", size);
})();
