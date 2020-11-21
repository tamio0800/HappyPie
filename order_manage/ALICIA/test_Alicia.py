import unittest
from Alicia_0611 import *

class TestALICIA(unittest.TestCase):
    
    def setUp(self):
        self.alicia = ALICIA()
        print("Set up alicia")

    def tearDown(self):
        print("Done test.")

    def test_failed(self):
        self.assertEqual(1+1, 3)

if __name__ == '__main__':
    unittest.main()