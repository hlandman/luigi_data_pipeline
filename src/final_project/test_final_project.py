import os

from final_project.cli import main
from unittest import TestCase
from final_project.tasks import *
from pset_utils.luigi.dask.target import *
from luigi import build


def test_main():
    assert main([]) == 0


class VisualTester(TestCase):

    def test_visualizer(self):
        """Tests that workflow outputs pdf file"""

        class GetBadTest(GetBadData):
            # Directory for local file
            DATA_ROOT = "testdata\\"

        class CleanerTest(DataCleaner):
            # Directory for clean test file
            CLEAN_PATH = "testdata\\cleaned\\"

            filename = Parameter(GetBadTest().filename)

            na_filler = Parameter("0")

            def requires(self):
                return GetBadTest()

        class EncoderTest(DataEncoder):
            # Directory for encoded test file
            ENCODED_PATH = "testdata\\encoded\\"

            filename = Parameter(CleanerTest().filename)

            cols = DictParameter({"cat": "quality", "dum": "none"})

            def requires(self):
                return CleanerTest()

        class VisualizerTest(DataVisualizer):
            # Directory for visual test file
            VISUAL_PATH = "testdata\\visualized\\"

            def requires(self):
                return EncoderTest()

        tester = VisualizerTest(encoder=True, xyvars={"x": "sulphates", "y": "quality"})
        build([tester], local_scheduler=True)

        path = 'testdata/visualized/figure.pdf'
        self.assertTrue(os.path.exists(path))
