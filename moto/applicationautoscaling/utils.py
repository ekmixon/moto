from urllib.parse import urlparse


def region_from_applicationautoscaling_url(url):
    domain = urlparse(url).netloc

    return domain.split(".")[1] if "." in domain else "us-east-1"
