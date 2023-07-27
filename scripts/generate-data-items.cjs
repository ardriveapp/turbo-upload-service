const { createData } = require("arbundles/src/ar-data-create");
const ArweaveSigner =
  require("arbundles/src/signing/chains/ArweaveSigner").default;
const EthereumSigner =
  require("arbundles/src/signing/chains/EthereumSigner").default;
const SolanaSigner =
  require("arbundles/src/signing/chains/SolanaSigner").default;
const fs = require("fs");
const bs58 = require("bs58");

(async () => {
  const target = process.env.DATA_ITEM_TARGET;
  const anchor = process.env.DATA_ITEM_ANCHOR;
  const walletFile = process.env.PRIV_KEY_FILE;
  const walletType = process.env.PRIV_KEY_TYPE ?? 1; /*Arweave*/
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

  if (!walletFile) {
    console.error("ERROR: Must provide a PRIV_KEY_FILE value!");
    process.exit(1);
  }

  if (!walletType) {
    console.error("ERROR: Must provide a PRIV_KEY_TYPE value!");
    process.exit(1);
  }

  if (!outfileName) {
    console.error("ERROR: Must provide an OUTFILE_NAME value!");
    process.exit(1);
  }

  const dataItemCreateOpts = {
    target,
    anchor,
    tags: [{ name: "Content-Type", value: "text/plain" }],
  };

  let signer;
  let dataItemSignerType;
  const keyFileBuffer = fs.readFileSync(walletFile, "utf-8");
  switch (+walletType) {
    case 1: {
      const keyFileData = JSON.parse(keyFileBuffer);
      signer = new ArweaveSigner(keyFileData);
      dataItemSignerType = "Arweave";
      break;
    }
    case 2: {
      // SOLANA
      // Read and parse the Solana key file
      const keyFileData = JSON.parse(keyFileBuffer);
      const secretKey = Uint8Array.from(keyFileData);

      // Convert the secret key to base58 format
      const secretKeyBase58 = bs58.encode(Buffer.from(secretKey));
      signer = new SolanaSigner(secretKeyBase58);
      dataItemSignerType = "Solana";
      break;
    }
    case 3: {
      signer = new EthereumSigner(keyFileBuffer.toString().trim()); // Expected to be a hex string for the pk
      dataItemSignerType = "Ethereum";
      break;
    }
  }

  const d = createData("hello", signer, dataItemCreateOpts);
  await d.sign(signer);
  const withTargetStr = target ? `with target '${target}' ` : "";
  const withAnchorStr = anchor
    ? `${target ? "and" : "with"} anchor '${anchor} ' `
    : "";
  console.log(
    `Writing ${dataItemSignerType}-signed data item ${withTargetStr}${withAnchorStr}to file ${outfileName}...`
  );
  fs.writeFileSync(outfileName, d.getRaw());
})();
