#!/usr/bin/env bash

set -euo pipefail

repository_root="$(git rev-parse --show-toplevel)"
cd "${repository_root}"

failed=0

report() {
    printf 'secret-scan: %s\n' "$*" >&2
    failed=1
}

while IFS= read -r -d '' path; do
    lower_path="${path,,}"
    case "/${lower_path}" in
        */.env|*/.env.*|*/config/secrets.json|*.key|*.pem|*.p12|*.pfx)
            report "forbidden tracked credential/private-key path: ${path}"
            ;;
    esac
done < <(git ls-files -z)

private_key_matches="$(
    git grep -n -I -E \
        -- '-----BEGIN (RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----' \
        -- . || true
)"
if [[ -n "${private_key_matches}" ]]; then
    report "private-key material found:"
    printf '%s\n' "${private_key_matches}" >&2
fi

credential_matches="$(
    git grep -n -I -i -E \
        -- "(api[_-]?key|access[_-]?token|auth[_-]?token|password|secret)[[:space:]]*[:=][[:space:]]*['\"]?[^[:space:]'\"]+" \
        -- CMakeLists.txt .github include src tests scripts \
        ':!scripts/check_no_secrets.sh' || true
)"
if [[ -n "${credential_matches}" ]]; then
    report "credential-looking assignment found:"
    printf '%s\n' "${credential_matches}" >&2
fi

if (( failed != 0 )); then
    exit 1
fi

printf 'secret-scan: clean\n'
