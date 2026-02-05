export type ValidatedUrl = {
  url: string;
  schema: string | undefined;
};

/**
 * Standard libpq connection parameters from PostgreSQL 17 documentation.
 * @see https://postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
 */
const VALID_LIBPQ_PARAMS: ReadonlySet<string> = new Set([
  "host",
  "hostaddr",
  "port",
  "dbname",
  "user",
  "password",
  "passfile",
  "connect_timeout",
  "client_encoding",
  "application_name",
  "fallback_application_name",
  "keepalives",
  "keepalives_idle",
  "keepalives_interval",
  "keepalives_count",
  "tcp_user_timeout",
  "sslmode",
  "sslcert",
  "sslkey",
  "sslrootcert",
  "sslpassword",
  "sslcompression",
  "sslcertmode",
  "sslkeylogfile",
  "sslcrl",
  "sslcrldir",
  "sslsni",
  "ssl_min_protocol_version",
  "ssl_max_protocol_version",
  "sslnegotiation",
  "require_auth",
  "channel_binding",
  "krbsrvname",
  "gssencmode",
  "gsslib",
  "gssdelegation",
  "options",
  "replication",
  "target_session_attrs",
  "load_balance_hosts",
  "service",
  "requirepeer",
  "min_protocol_version",
  "max_protocol_version",
  "oauth_issuer",
  "oauth_client_id",
  "oauth_client_secret",
  "oauth_scope",
  "scram_client_key",
  "scram_server_key",
]);

export function validateConnectionUrl(input: string | URL): ValidatedUrl {
  const url = new URL(input.toString());

  let schema: string | undefined;
  const keysToDelete: string[] = [];

  for (const [key, value] of url.searchParams) {
    if (key === "schema") {
      schema = value || undefined;
      keysToDelete.push(key);
    } else if (!VALID_LIBPQ_PARAMS.has(key)) {
      console.warn(
        `[@onreza/prisma-adapter-bun] Non-standard PostgreSQL connection parameter "${key}" was removed from URL. Use only standard libpq parameters: https://postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS`,
      );
      keysToDelete.push(key);
    }
  }

  for (const key of keysToDelete) {
    url.searchParams.delete(key);
  }

  return { schema, url: url.toString() };
}
