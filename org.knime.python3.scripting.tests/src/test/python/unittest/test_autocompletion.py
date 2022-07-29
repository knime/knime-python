"""
To run: python -m unittest discover
"""
import unittest

from autocompletion_utils import disable_autocompletion


class AutocompletionTest(unittest.TestCase):
    """
    The disable_autocompletion function expects a string as input, where the
    cursor position is assumed to be at the last index (the position of the dot).

    Autocompletion is expected to be disabled in strings and comments, but enabled
    inside curly braces of f-strings.
    """

    def setUp(self):
        self.all_quotes = ['"', "'", '"""', "'''"]
        self.single_quotes = ['"', "'"]
        self.triple_quotes = ['"""', "'''"]

    def test_default_behaviour(self):
        # cases with no strings in the current line
        self.assertFalse(disable_autocompletion("."))
        self.assertFalse(disable_autocompletion("var = len(np."))

    def test_strings(self):
        # permutations of different quotation marks in simple strings
        for quote_1 in self.all_quotes:
            self.assertTrue(disable_autocompletion(f"{quote_1}."))
            self.assertTrue(disable_autocompletion(f"{quote_1}string."))
            self.assertTrue(disable_autocompletion(f"some text {quote_1}."))
            self.assertTrue(disable_autocompletion(f"{quote_1}{quote_1}{quote_1}."))

            self.assertFalse(disable_autocompletion(f"{quote_1}{quote_1}."))
            self.assertFalse(disable_autocompletion(f"{quote_1}string{quote_1}."))

        # single quotes inside triple quotes
        for triple_quote in self.triple_quotes:
            for single_quote in self.single_quotes:
                self.assertTrue(
                    disable_autocompletion(f"{triple_quote}{single_quote}.")
                )
                self.assertTrue(
                    disable_autocompletion(f"{triple_quote}{single_quote}.")
                )

                self.assertFalse(
                    disable_autocompletion(
                        f"{triple_quote}{single_quote}{triple_quote}."
                    )
                )

        self.assertFalse(disable_autocompletion("""'"'."""))
        self.assertFalse(disable_autocompletion(""""'"."""))
        self.assertFalse(disable_autocompletion("""'''"'"'''."""))
        self.assertFalse(disable_autocompletion('''"""'"'""".'''))

    def test_comments(self):
        self.assertTrue(disable_autocompletion("#."))
        self.assertTrue(disable_autocompletion("some text #."))

        for quote in self.all_quotes:
            self.assertTrue(disable_autocompletion(f"{quote}#."))
            self.assertTrue(disable_autocompletion(f"#f{quote}{{."))
            self.assertTrue(disable_autocompletion(f"{quote}{quote}#."))
            self.assertTrue(disable_autocompletion(f"{quote}#{quote}{quote}."))

            self.assertFalse(disable_autocompletion(f"{quote}#{quote}."))

        # self.assert

    def test_f_strings(self):
        for quote in self.all_quotes:
            self.assertTrue(disable_autocompletion(f"f{quote}{{}}."))
            self.assertTrue(disable_autocompletion(f"f{quote}{{some text}}."))
            self.assertTrue(disable_autocompletion(f"f{quote}{{a}}{{b}}."))

            self.assertFalse(disable_autocompletion(f"f{quote}{{."))
            self.assertFalse(disable_autocompletion(f"f{quote}string{{text}}{quote}."))

        # failing tests
        # self.assertTrue(disable_autocompletion(f"""f"f'{{."""))
        # self.assertTrue(disable_autocompletion(f"""f"{{'."""))


if __name__ == "__main__":
    unittest.main()
