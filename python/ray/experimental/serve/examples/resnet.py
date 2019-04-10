from __future__ import absolute_import

import ray
from ray.experimental.serve import RayServeMixin, batched_input

from torchvision import models, transforms

import urllib.request, json

@ray.remote
class PyTorchResNet50(RayServeMixin):
    """Returns the result of running ResNet50 using
    the PyTorch framework.
    """

    def __init__(self):
        self.model = models.resnet50(pretrained=True)
        normalize = transforms.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225]
        )
        self.preprocess = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            normalize
        ])
        with urllib.request.urlopen("https://s3.amazonaws.com/deep-learning-models/image-models/imagenet_class_index.json") as url:
            class_idx = json.loads(url.read().decode())
            self.idx2label = [class_idx[str(k)][1] for k in range(len(class_idx))]
        
    
    @batched_input
    def __call__(self, batched_input):
        import io
        from PIL import Image
        import torch
        import torch.nn as nn

        # We first prepare a batch from `imgs`
        img_tensors = []
        for img in imgs:
            img_tensor = preprocess(Image.open(io.BytesIO(img)))
            img_tensor.unsqueeze_(0)
            img_tensors.append(img_tensor)
            img_batch = torch.cat(img_tensors)

        # We perform a forward pass
        with torch.no_grad():
            model_output = model(img_batch)

        image_labels = []
        for out in model_output:
            prob = nn.Softmax()(out).numpy()
            prob = np.squeeze(prob)
            results = out.data.numpy()
            top5 = np.argpartition(results, -5)[-5:]
            top5 = top5[np.argsort(results[top5])]
            image_labels += [[[labels[i], prob[i]] for i in top5[::-1]]]
        return image_labels
