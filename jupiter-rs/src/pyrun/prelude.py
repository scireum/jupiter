
try: VERSION
except NameError: VERSION = "-"

print("VERSION: "+ VERSION)
print("READY!")

import sys
import json

def handle_call(query):
    try:
        json_query = json.loads(query)
        json_response = kernel(json_query)
        print(json.dumps(json_response, indent=None, separators=(",",":")))
    except Exception as error:
        print(json.dumps({ "error": str(error), "type": str(type(error)) }, indent=None, separators=(",",":")))

for line in sys.stdin:
    if line.startswith("EXIT!"):
        print("BYE!")
        break
    else:
        handle_call(line)
