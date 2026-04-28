from __future__ import annotations

import unittest

import numpy as np

from ibkr_trader.rl.inference_vector import RunnerSymbolState
from ibkr_trader.rl.inference_vector import assemble_dqn_observation_vector
from ibkr_trader.rl.inference_vector import valid_action_mask


class RLInferenceVectorTests(unittest.TestCase):
    def test_assembles_vector_in_bucket_dqn_order(self) -> None:
        observation = {
            "bar_count": 2,
            "model_decision": {"usable_bar_count": 2},
            "features": {
                "static_features_ready": True,
                "static_features": [10.0, 11.0],
                "base_dynamic": [[1.0, 2.0], [3.0, 4.0]],
                "extra_dynamic": [[5.0], [6.0]],
                "history_features": [7.0, 8.0],
                "path_feature_stack": [[0.1, 0.2], [0.3, 0.4]],
            },
            "phase1_bars": [
                {"open": "100.0"},
                {"open": "101.0"},
            ],
            "pricing_context": {
                "prev_close": "99.0",
                "session_open": "100.0",
            },
        }

        vector = assemble_dqn_observation_vector(
            observation,
            state=RunnerSymbolState(),
            model_side="LONG",
            path_pad_length=4,
            expected_obs_dim=30,
        )

        self.assertEqual(vector.dtype, np.float32)
        np.testing.assert_allclose(vector[:5], np.array([10.0, 11.0, 3.0, 4.0, 6.0]))
        runtime_start = 5
        runtime_end = runtime_start + 15
        np.testing.assert_allclose(
            vector[runtime_start:runtime_end],
            np.array([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]),
        )
        np.testing.assert_allclose(vector[runtime_end : runtime_end + 2], np.array([7.0, 8.0]))
        np.testing.assert_allclose(
            vector[-8:],
            np.array([0.1, 0.3, 0.0, 0.0, 0.2, 0.4, 0.0, 0.0]),
        )

    def test_valid_flat_mask_matches_bucket_action_space(self) -> None:
        action_names = [
            "skip",
            "wait",
            "market_entry",
            "cancel_entry",
            "exit_market",
            "clear_exit",
            "entry_prevclose_-50bp",
            "exit_tp_200bp",
        ]

        mask = valid_action_mask(action_names, RunnerSymbolState())

        self.assertEqual(
            mask.tolist(),
            [True, True, True, False, False, False, True, False],
        )


if __name__ == "__main__":
    unittest.main()
