from user_agents import parse
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType

def get_browser_family(ua): return parse(ua).browser.family if ua else None
def get_browser_version(ua): return parse(ua).browser.version_string if ua else None
def get_os_family(ua): return parse(ua).os.family if ua else None
def get_os_version(ua): return parse(ua).os.version_string if ua else None
def get_device_family(ua): return parse(ua).device.family if ua else None
def get_device_brand(ua): return parse(ua).device.brand if ua else None
def get_device_model(ua): return parse(ua).device.model if ua else None

def get_is_mobile(ua): return parse(ua).is_mobile if ua else False
def get_is_tablet(ua): return parse(ua).is_tablet if ua else False
def get_is_touch_capable(ua): return parse(ua).is_touch_capable if ua else False
def get_is_pc(ua): return parse(ua).is_pc if ua else False
def get_is_bot(ua): return parse(ua).is_bot if ua else False

browser_family_udf    = udf(get_browser_family, StringType())
browser_version_udf   = udf(get_browser_version, StringType())
os_family_udf         = udf(get_os_family, StringType())
os_version_udf        = udf(get_os_version, StringType())
device_family_udf     = udf(get_device_family, StringType())
device_brand_udf      = udf(get_device_brand, StringType())
device_model_udf      = udf(get_device_model, StringType())

is_mobile_udf         = udf(get_is_mobile, BooleanType())
is_tablet_udf         = udf(get_is_tablet, BooleanType())
is_touch_capable_udf  = udf(get_is_touch_capable, BooleanType())
is_pc_udf             = udf(get_is_pc, BooleanType())
is_bot_udf            = udf(get_is_bot, BooleanType())
