from locust import HttpLocust, TaskSet, task
import json

class UserBehavior(TaskSet):

    @task
    def test_auction(self):
        with self.client.get("/pub?tag_id=54f8df2e6bcc85d9653becfb", catch_response=True) as response:
            if json.loads(response.content).has_key('default'):
                response.failure('default ad served, all bids timed out')


class TestUser(HttpLocust):
    task_set = UserBehavior
    host = 'http://130.211.132.2:5000'
    min_wait=1
    max_wait=2000

class BidderBehavior(TaskSet):

    @task
    def test_bidder(self):
        data = {"id": "bf150110-becf-11e4-b019-bfbc30412a79",
                "cur": ["USD"],
                "at": 2,
                "device": {
                    "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.115 Safari/537.36",
                    "ip": "73.47.234.128",
                    "geo": {
                        "type": 2,
                        "lat": 42.3331,
                        "lon": -71.0957,
                        "country": "USA",
                        "region": "MA",
                        "city": "Boston",
                        "zip": "02120",
                        "metro": 506
                    }
                },
                "user": {
                    "id": "5fc9ffa0-bd29-11e4-8c62-014ce6b16299"
                },
                "site": {
                    "id": "102855",
                    "cat": ["IAB3-1"],
                    "domain": "130.211.132.2:5000",
                    "page": None,
                    "publisher": {
                      "id": "8953",
                      "name": "130.211.132.2",
                      "cat": ["IAB3-1"],
                      "domain": "130.211.132.2:5000"
                    }
                  },
                "imp": [{
                            "id": 186849422181208,
                            "bidfloor": 0.03,
                            "banner": {
                                "h": 250,
                                "w": 300
                            }
                        }]
                }
        self.client.post('/bid?bidder_id=10', data)

# class TestBidder(HttpLocust):
#     task_set = BidderBehavior
#     host = 'http://104.154.59.193:5100'
#     min_wait = 1
#     max_wait = 2000