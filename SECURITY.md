# Security Policy

## Supported Versions

The following versions of this project are currently supported with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0.0 | :x:                |

## Reporting a Vulnerability

We take the security and integrity of our OLAP analytical data warehouse seriously. If you discover a vulnerability, SQL injection risk, or security flaw, please report it responsibly.

### Disclosure Process

1. **Private Reporting**: Please **DO NOT** file a public GitHub issue. Send your vulnerability report directly via email to **[tayyabmasood911@gmail.com](mailto:tayyabmasood911@gmail.com)** with the subject line `[SECURITY] clickhouse-olap-analytics-ingestion Vulnerability`.
2. **Details to Include**:
   - Description of the vulnerability and attack vector.
   - Minimal reproduction script or SQL snippet.
   - Affected database engine, table DDL, or ingestion script.
   - Proposed mitigation or remediation patch if available.
3. **Response Timelines**:
   - **Initial Acknowledgement**: Within 24 hours.
   - **Triage & Assessment**: Within 72 hours.
   - **Remediation & Advisory Release**: Coordinated fix within 14 business days.

## Security Controls & Best Practices

- **Zero-Trust Access & RBAC**: Ingestion consumers and analytics users operate with isolated SQL user accounts with strict table-level read/write permissions.
- **SQL Injection Prevention**: Load test ingestion scripts utilize parameterized binary or JSONEachRow protocol ingestion formats.
- **Network Boundaries**: ClickHouse native port `9000` and HTTP port `8123` must be shielded behind internal private VPC networks with TLS enabled.

## Safe Harbor

We consider security research conducted in good faith under this policy to be authorized. We will not pursue legal action against researchers who adhere to responsible disclosure principles.
