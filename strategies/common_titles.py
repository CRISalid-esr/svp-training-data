# declare a list of common titles for the strategies
# this title ("Préface", "Introduction") are common titles that are not relevant for the similarity
from simple_duplicate_detector import SimpleDuplicateDetector

COMMON_TITLES = ["Préface", "Introduction", "Preface"]


def common_titles(titles):
    return all(
        SimpleDuplicateDetector.normalize_text(title) in
        [SimpleDuplicateDetector.normalize_text(common_title) for common_title in COMMON_TITLES]
        for title in titles)
