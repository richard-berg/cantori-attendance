# cantori-attendance
Automate attendance reports

## Preview reports locally

Sign in with an Azure identity that can read the Monday and ChoirGenius secrets
from Key Vault (for example, via `az login`). In VS Code, run one of the
**Preview attendance report**, **Preview projected attendance report**,
**Preview consistency report**, **Preview member nag**,
**Preview looking ahead report**, or **Preview welcome email** launch configurations.
Alternatively, run `.venv/Scripts/python.exe preview_report.py attendance_report`
from the project root (use the Python executable in `.venv/bin` on Unix).

The preview fetches live data and writes the email HTML to `previews/` without
using Microsoft Graph or sending mail. It renders even if the scheduled report
would not be sent today. Member nags and welcome emails preview only the first
eligible member; if none is eligible (or, for the looking ahead report, if the
current cycle is the last of the season), no new file is written. Preview files
are ignored by git because they may contain personal data. Open the generated
file in a browser to inspect it.

## Looking ahead report

Twice per concert cycle (halfway between the cycle's first rehearsal and its
concert, and 14 days before the concert), a report about the next cycle is sent
to attendance@, Richard, and Janara. It lists next-cycle "Maybes", returning
members, accepted auditionees who are sitting out the current cycle, next-cycle
singers who aren't active in ChoirGenius, and likely departures. On the second
send date, returning members and auditionees joining the next cycle each receive
a welcome email. Nothing is sent during the last cycle of the season.
