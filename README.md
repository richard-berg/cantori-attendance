# cantori-attendance
Automate attendance reports

## Preview reports locally

Sign in with an Azure identity that can read the Monday and ChoirGenius secrets
from Key Vault (for example, via `az login`). In VS Code, run one of the
**Preview attendance report**, **Preview projected attendance report**,
**Preview consistency report**, or **Preview member nag** launch configurations.
Alternatively, run `.venv/Scripts/python.exe preview_report.py attendance_report`
from the project root (use the Python executable in `.venv/bin` on Unix).

The preview fetches live data and writes the email HTML to `previews/` without
using Microsoft Graph or sending mail. It renders even if the scheduled report
would not be sent today. Member nags preview only the first eligible member;
if none is eligible, no new file is written. Preview files are ignored by git
because they may contain personal data. Open the generated file in a browser
to inspect it.
