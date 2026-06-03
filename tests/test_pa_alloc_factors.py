"""PA allocation factor helpers — no 1.0 pass-through when monthly gathered gas is zero."""

import unittest

from monthly_loader_gui import (
    compute_wh_to_s2_alloc_factor,
    compute_wh_to_sales_cond_alloc_factor,
)


class TestPaAllocFactors(unittest.TestCase):
    def test_s2_factor_none_when_gathered_zero(self):
        self.assertIsNone(compute_wh_to_s2_alloc_factor(100.0, 0.0))
        self.assertIsNone(compute_wh_to_s2_alloc_factor(100.0, None))

    def test_s2_factor_ratio_when_gathered_positive(self):
        self.assertEqual(compute_wh_to_s2_alloc_factor(50.0, 100.0), 0.5)
        self.assertEqual(compute_wh_to_s2_alloc_factor(0.0, 100.0), 0.0)

    def test_cond_factor_none_when_wh_zero(self):
        self.assertIsNone(compute_wh_to_sales_cond_alloc_factor(10.0, 0.0))

    def test_cond_factor_ratio_when_wh_positive(self):
        self.assertEqual(compute_wh_to_sales_cond_alloc_factor(5.0, 20.0), 0.25)


if __name__ == "__main__":
    unittest.main()
