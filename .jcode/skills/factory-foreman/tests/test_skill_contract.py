import re
import unittest
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parents[1]
SKILL = SKILL_DIR / "SKILL.md"
REFERENCE = SKILL_DIR / "REFERENCE.md"

APPROVAL_SENTENCE = (
    "Never execute a mutating Rat Kingdom command unless a later user message "
    "explicitly approves the exact command rendered in this conversation."
)

TRIGGERS = [
    "Rat Kingdom factory",
    "fleet health",
    "RK inbox",
    "workflow failures",
    "factory triage",
    "dispatch work",
    "software factory",
]

MUTATION_PHRASES = [
    "automatically execute",
    "auto-execute",
    "run without approval",
    "execute immediately",
    "dispatch automatically",
]


class FactoryForemanSkillContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.skill_text = SKILL.read_text(encoding="utf-8")
        cls.reference_text = REFERENCE.read_text(encoding="utf-8")

    def test_skill_has_valid_repository_local_frontmatter(self):
        self.assertTrue(self.skill_text.startswith("---\n"))
        frontmatter = self.skill_text.split("---\n", 2)[1]
        self.assertRegex(frontmatter, r"(?m)^name:\s*factory-foreman\s*$")
        self.assertRegex(frontmatter, r"(?m)^description:\s*.+$")
        self.assertRegex(frontmatter, r"(?m)^triggers:\s*$")
        for trigger in TRIGGERS:
            self.assertIn(f"- {trigger}", frontmatter)

    def test_all_required_trigger_terms_are_present(self):
        combined = f"{self.skill_text}\n{self.reference_text}"
        for trigger in TRIGGERS:
            self.assertIn(trigger, combined)

    def test_skill_is_under_500_lines(self):
        self.assertLess(len(self.skill_text.splitlines()), 500)

    def test_default_workflow_is_read_only_and_triage_first(self):
        self.assertIn(
            "python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py triage --repo <repo> --format markdown",
            self.skill_text,
        )
        self.assertRegex(self.skill_text.lower(), r"read-only.*default|default.*read-only")
        self.assertIn("Run read-only triage first", self.skill_text)

    def test_exact_approval_language_is_verbatim(self):
        self.assertIn(APPROVAL_SENTENCE, self.skill_text)

    def test_skill_does_not_authorize_automatic_mutation(self):
        lowered = self.skill_text.lower()
        for phrase in MUTATION_PHRASES:
            self.assertNotIn(phrase, lowered)

    def test_markdown_links_are_only_one_level_deep(self):
        for path in re.findall(r"\[[^\]]+\]\(([^)]+)\)", self.skill_text):
            if "://" in path or path.startswith("#"):
                continue
            self.assertFalse(path.startswith("../"), path)
            self.assertLessEqual(len(Path(path).parts), 1, path)

    def test_authority_wording_requires_later_user_approval_and_exact_validation(self):
        required = [
            "Stop and request approval",
            "later user message explicitly approves",
            "validate-proposal",
            "execute only the validated argv",
            "requires a new proposal and new approval",
            "workflow watch --json is NDJSON",
        ]
        for phrase in required:
            self.assertIn(phrase, self.skill_text)


if __name__ == "__main__":
    unittest.main()
