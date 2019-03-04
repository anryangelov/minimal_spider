import json

import bottle

import constants
from most_wanted_mongo import MWMongo


app = bottle.Bottle()


reply_404 = json.dumps({'message': 'Not Found'})

client = MWMongo(
    host=constants.MONGO_HOST,
    port=constants.MONGO_PORT,
    db_name=constants.MONGO_DB
)
most_wanted_collec = client.connect()


@app.error(404)
def error_handler_404(error):
    bottle.response.content_type = 'application/json'
    return reply_404


@app.route('/most_wanted', method='GET')
def list_profiles():
    items = most_wanted_collec.get_all_id_str()
    bottle.response.content_type = 'application/json'
    return json.dumps(items, indent=4)


@app.route('/most_wanted/<object_id>', method='GET')
def describe_profile(object_id):
    item = most_wanted_collec.get_by_object_id(object_id)
    if not item:
        raise bottle.HTTPResponse(
            status=404,
            body=reply_404,
            content_type='application/json')
    return item


if __name__ == '__main__':
    app.run()
