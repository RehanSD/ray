from __future__ import absolute_import

import ray
from ray.experimental.serve import RayServeMixin, batched_input

@ray.remote
class NoOp(RayServeMixin):
    """NoOp actor.
    """

    @batched_input
    def __call__(self, input_batch):
        return
