from synthetic.warmers.base import BaseWarmer
from synthetic.warmers.amazon_connect import ConnectWarmer

WARMERS = {
    "amazon-connect": ConnectWarmer,
}
