import unittest
from pathlib import Path

from question_bank.db import connect, initialize_database
from question_bank.importer import import_bundle


class InitAndImportTests(unittest.TestCase):
    def test_initialize_and_import_bundle(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        bundle = project_root / "samples" / "demo_bundle"
        db_path = project_root / "data" / "test_init_import.db"
        if db_path.exists():
            db_path.unlink()
        initialize_database(db_path)
        result = import_bundle(bundle, db_path=db_path, dry_run=False)
        self.assertEqual(result["status"], "committed")
        self.assertEqual(result["imported_questions"], 3)
        self.assertEqual(result["imported_workbooks"], 1)
        with connect(db_path) as conn:
            paper = conn.execute(
                "SELECT edition, paper_type, paper_latex_path, paper_latex_source, question_index_json FROM papers WHERE paper_id = ?",
                ("CPHOS-18-REGULAR-DEMO",),
            ).fetchone()
            question = conn.execute(
                "SELECT paper_index, latex_path, latex_source, answer_latex_path, answer_latex_source, search_text FROM questions WHERE question_id = ?",
                ("QB-2024-T-01",),
            ).fetchone()
            workbook = conn.execute(
                "SELECT workbook_kind, source_filename, file_size, workbook_blob FROM score_workbooks WHERE workbook_id = ?",
                ("WB-CPHOS-18-DEMO-INDEX",),
            ).fetchone()
            question_count = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
            asset_count = conn.execute("SELECT COUNT(*) FROM question_assets").fetchone()[0]
        self.assertEqual(question_count, 3)
        self.assertEqual(asset_count, 2)
        self.assertEqual(paper["edition"], 18)
        self.assertEqual(paper["paper_type"], "regular")
        self.assertIn("samples/demo_bundle/latex/papers/demo-paper.tex", paper["paper_latex_path"])
        self.assertIn("\\documentclass", paper["paper_latex_source"])
        self.assertEqual(question["paper_index"], 1)
        self.assertIn("samples/demo_bundle/latex/questions/QB-2024-T-01.tex", question["latex_path"])
        self.assertIn("samples/demo_bundle/latex/answers/QB-2024-T-01-answer.tex", question["answer_latex_path"])
        self.assertIn("mechanics", question["search_text"])
        self.assertIn("\\begin{problem}", question["latex_source"])
        self.assertIn("\\begin{solution}", question["answer_latex_source"])
        self.assertEqual(workbook["workbook_kind"], "paper_registry")
        self.assertEqual(workbook["source_filename"], "demo_score_index.xlsx")
        self.assertGreater(workbook["file_size"], 0)
        self.assertGreater(len(workbook["workbook_blob"]), 0)


if __name__ == "__main__":
    unittest.main()
