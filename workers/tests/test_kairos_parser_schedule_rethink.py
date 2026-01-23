import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from workers.kairos.main import parse_kairos_headers, _missing_required_fields, _validated_analysis


class KairosParserScheduleRethinkTests(unittest.TestCase):
    def test_parses_schedule_rethink_no(self):
        raw = """STRATEGIC_NARRATIVE
He is warm.

CONVERSATION_CRITICALITY
LABEL: Low
REASONING: Routine.

TACTICAL_SIGNALS
- "ok"

PSYCHOLOGICAL_LEVERS
Keep it light.

RISKS
- Latent boredom.

TURN_MICRO_NOTE
SUMMARY: Low-signal reply.

MOMENT_COMPASS
Low energy, maintenance.

SCHEDULE_RETHINK
NO

### END"""
        parsed, err = parse_kairos_headers(raw)
        self.assertIsNone(err)
        self.assertEqual("NO", parsed["SCHEDULE_RETHINK"].strip())
        validated = _validated_analysis(parsed)
        self.assertEqual([], _missing_required_fields(validated))

    def test_parses_schedule_rethink_yes_with_reasoning(self):
        raw = """STRATEGIC_NARRATIVE
He is leaning in.

CONVERSATION_CRITICALITY
LABEL: High
REASONING: He is opening up.

TACTICAL_SIGNALS
- He says "I never tell anyone this".

PSYCHOLOGICAL_LEVERS
Validation and safety.

RISKS
- If we go cold, he shuts down.

TURN_MICRO_NOTE
SUMMARY: Vulnerable disclosure.

MOMENT_COMPASS
He wants closeness and reassurance.

SCHEDULE_RETHINK
YES
The Fan is unusually vulnerable right now and the tone is deep.
If the Creator disappears because of an upcoming plan, it will likely feel like rejection.
Staying present would strengthen reliance and trust at a critical moment.
This is worth canceling or renaming an upcoming plan to protect momentum.

### END"""
        parsed, err = parse_kairos_headers(raw)
        self.assertIsNone(err)
        self.assertTrue(parsed["SCHEDULE_RETHINK"].lstrip().startswith("YES"))
        validated = _validated_analysis(parsed)
        self.assertEqual([], _missing_required_fields(validated))


if __name__ == "__main__":
    unittest.main()

