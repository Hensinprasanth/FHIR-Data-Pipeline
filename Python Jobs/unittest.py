# -*- coding: utf-8 -*-
"""
Created on Fri 23 10:01:00 2024

@author: Hensin Prasanth M
"""
import unittest
from main import normalize_fhir_data
import pandas as pd

class TestNormalizeFHIRData(unittest.TestCase):

    def assertDataFramesEqual(self, df1, df2):
        self.assertEqual(df1.to_string(), df2.to_string())

    def test_normalize_fhir_data_empty(self):
        # Test when the input FHIR data is empty
        fhir_data = {}
        result = normalize_fhir_data(fhir_data)
        self.assertEqual(result, {})

    def test_normalize_fhir_data_single_resource(self):
        # Test when the input FHIR data contains a single resource
        fhir_data = {
            "entry": [
                {
                    "resource": {
                        "resourceType": "Patient",
                        "id": "1",
                        "name": [{"family": "Doe", "given": ["John"]}],
                        "gender": "male"
                    }
                }
            ]
        }
        result = normalize_fhir_data(fhir_data)
        print ("see this pls")
        print (result)
        expected_result = {
            "Patient": pd.DataFrame({
                "resourceType": ["Patient"],
                "id": ["1"],
                "name.family": ["Doe"],
                "name.given": ["John"],
                "gender": ["male"]
            })
        }
        self.assertEqual(len(result), len(expected_result))
        self.assertDataFramesEqual(result["Patient"], expected_result["Patient"])

if __name__ == '__main__':
    unittest.main()