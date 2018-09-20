curl - X
GET \
    'https://opticx.optimumenergyco.com/api/2016.06/datapoint-histories?endDate=2017-10-01T05:59:59.999Z&names=sum(TotalkWh)&resolution=hour&sid=c:testcustomerdemo.s:demo2-edge&startDate=2017-09-01T06:00:00.000Z' \
    - H
'authorization: Bearer 1a94fca8-b3fa-42df-9dd5-faa9ce74f447' \
- H
'cache-control: no-cache' \
- H
'content-type: application/json' \
- H
'postman-token: d7a08eef-8cb8-24a9-d85a-844bb0c0eb1a'




if __name__ == '__main__':
