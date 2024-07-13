from colorama import Style
import itertools
import sys
import time


def print_color(text, color):
    print(f"{color}{text}{Style.RESET_ALL}")
