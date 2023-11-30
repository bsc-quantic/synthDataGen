import random

from typing import Dict, List, Tuple

import pandas as pd
from scipy.stats import truncnorm

class Sampling:

    _availProbDistibutions: List = ["'truncnorm'"]

    @staticmethod
    def _getMeanAndStdForAxis(df: pd.DataFrame, axis: int) -> Tuple[List, List]:
        means: pd.Series = df.mean(axis = axis)
        stds: pd.Series = df.std(axis = axis, ddof = 0)

        return (means, stds)
    
    @staticmethod
    def _getSamples_truncnorm(df: pd.DataFrame, numberOfSamples: int, means: List[float], stds: List[float]) -> pd.DataFrame:
        resultingDataFrame: pd.DataFrame = pd.DataFrame()

        for index, mu, sigma in zip(df.index, means, stds):
            lowerBound = 0
            upperBound = mu + 2*sigma

            pdf = truncnorm((lowerBound - mu) / sigma,
                            (upperBound - mu) / sigma,
                            loc = mu, scale = sigma)

            samples = pdf.rvs(numberOfSamples)
            resultingDataFrame[index] = samples
        
        return resultingDataFrame

    @staticmethod
    def getSamples(df: pd.DataFrame, numberOfSamples: int = None, probDistribution: str = None) -> pd.DataFrame:
        """Gets a number of samples for every column in the provided DataFrame. A truncated normal probability distribution is used to do so.

        :param pandas.DataFrame df: the input DataFrame to be considered.
        :param int numberOfSamples: the number of samples that will be returned (number of rows).
        :param str probDistribution: a string defining the probability distribution to be used. For instance "truncnorm".
        :returns pandas.DataFrame:
        """

        if probDistribution == "truncnorm":
            means, stds = Sampling._getMeanAndStdForAxis(df, 1)
            return Sampling._getSamples_truncnorm(df, numberOfSamples, means, stds)
        else:
            raise ValueError("Probability distribution '" + probDistribution +"' not available for sampling. Please choose one of the following: " + ', '.join(Sampling._availProbDistibutions))
        

class ProbDistributions:

    @staticmethod
    def getUniform(min: int | float, max: int | float, n: int) -> List[int|float]:
        """ Returns an n-array of floats following a uniform distribution between (min,max) values

        :param int | float min: minimum parameter for the uniform distribution
        :param int | float max: maximum parameter for the uniform distribution
        :param int n: number of values to generate
        :returns List[int|float]
        """
        if isinstance(min, int) and isinstance(max, int):
            return [ random.randint(min, max) for x in range(n) ]
        elif isinstance(min, float) and isinstance(max, float):
            return [ random.uniform(min, max) for x in range(n) ]
        else:
            raise ValueError("'min' and 'max' arguments must be either both integers or both floats.")