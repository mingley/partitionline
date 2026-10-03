# Independent signed JWT fixtures and Apache components

The controlled-time corpus contains 70 public synthetic tokens: 65 have an
independently verified cryptographic proof, 12 satisfy the selected local policy,
and 58 are rejected by that policy. RSA keys exercise both supported size
boundaries, 2048 and 4096 bits; P-256 signatures use the 64-byte JOSE R||S format.
OpenSSL generates and verifies signatures. No partitionline decoder supplies a
signature, expected verdict, or fixture body.

`partitionline-broker/tests/fixtures/oidc/fixtures.json` declares the exact issuer
`https://issuer.example/realm`, audience `partitionline`, controlled epoch
1800000000, each validation epoch, and each policy decision. The default requires
protected `typ=at+jwt`. The explicit alternate profile requires the signed
`token_use=access` claim. Header/payload sidecars preserve exact UTF-8, including
deliberately malformed or duplicate JSON. `public_key_sha256` hashes DER SPKI;
`public_jwk_sha256` hashes the exact compact JSON JWK produced by the generator.
Compact files contain no newline. Signing-input hashes cover the first two
segments, including the malformed four-segment control. Cryptographic validity
is checked against the named independent signer key, separately from the
declared header algorithm/kid and strict token syntax. Thus an unsupported
algorithm, unknown kid, or padded proof can have a valid underlying proof.

Private synthetic signing keys exist only in scratch. Public PEM/SPKI and
public-only JWKS are retained. `generate-fixtures.py` creates fresh scratch keys
if necessary; reusing them reproduces RSA bytes, while EC signatures are
randomized. Each actual fixture is checksum-pinned. This reproducibility boundary
is explicit; a fresh key generation is not claimed to recreate identical token
bytes. `check-fixtures.py` rechecks exact compact/sidecar/key bytes and all 70
OpenSSL proof outcomes. Its positive baseline and nine corrupted-input controls
pass in `development/fixture-counterexamples.json`.

`ApacheJwtProbe.java` invokes unmodified public `BrokerJwtValidator` and
`DefaultJwtValidator` constructors, configures issuer/audience/skew, and uses the
actual `VerificationKeyResolverFactory` file resolver with an explicit URL/file
allowlist. Official release distributions and source archives are checksum-pinned;
retained source includes LICENSE, NOTICE, validators, factory/resolver, configuration,
claim helpers and upstream tests. All three embedded client commit IDs match the
peeled source pins. No API adaptations were needed. Strict compilation uses
`-Xlint:all -Werror`.

The first component matrix executed 420 validations (70 tokens × two configured
validators × three releases), passed 174 mandatory component assertions, and
rejected three deliberately wrong valid-token expectations. The official
validators use the real JVM clock; a separate freshly signed corpus records its
actual issuance epoch and every observed validation epoch. The fixed local
validation epochs do not change the Apache clock. Apache accepts several tokens
the stricter local policy rejects, including wrong/missing access types, ID-token
discriminators, long lifetimes, future issued-at values, fractional expiration,
large fields, unconfigured authority headers and padded proofs. The initial
near-expiry observation also changes as the real clock advances. Actual outcomes
and every difference are retained per release; no full policy equivalence is
claimed.

The first generator attempt retained a faulty tamper control: changing the final
base64url character can alter unused bits while leaving decoded proof bytes
unchanged. OpenSSL correctly detected that the proof remained valid. The corrected
control changes an actual decoded signature byte. No Rust product failure is
inferred from this generator error. The production worker separately retained its
first signing-input sidecar assumption failure for the malformed fourth segment.

Run the components from a fresh work/output directory:

```sh
python3 prepare-and-run.py --work /path/to/fresh-work \
  --output /path/to/fresh-output --private-work /path/to/scratch-keys \
  --source-archives /path/to/pinned-source-archives \
  --distributions /path/to/pinned-official-distributions
```

This component evidence does not qualify HTTPS discovery, JWKS rotation,
revocation, network outages, SASL socket sessions, post-authentication authority,
or production OIDC. Those remain separate KL11-37 acceptance evidence.
