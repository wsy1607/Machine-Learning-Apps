#the structure of the logging systems: only design for analytics and machine learning purpose


#all tables:

#general logs
#1: sessions, web session info
#2: pageviews, web view page info

#events
#3: registrations, user registration info
#4: subscriptions, user subscription (beer, vendor, other user) info
#5: searches, internal search info (user search queries and results)
#6: clicks, user clicks info
#7: carts, user carts info
#8: checkouts, user checkouts info (checkout page) per item
#9: orders, user orders info
#10: ratings, beer ratings info

#special notes for columns
#symbol $ in the beginning: may be a trivial column
#symbol ? in the beginning: not quite sure about this column

#format: (symbol) column name, data type, data constraint and description; one column per line


#discussions & questions:
#1: for virtualization in Kibana, is it allowed to join two or multiple tables to make one chart?
#2: how to define clicks as an event, is 'subscription' or 'add to cart' also a 'click' event?
#3: only regular beers hve been considered, how about gift cards?


#sample dashboard charts

#general growth
#1: sessions/page views/subscriptions/orders/sales ... growth per day
#2: special beer items/vendor beers sales growth per day

#top camparison
#3: most viewed/sold beers
#4: most viewed vendors
#5: top external traffic source
#6: top search terms
#7: most active users

#other statistics
#8: sales/clicks/subscriptions/sessions of geographics
#9: top beer activities: clicks/subscriptions/ratings
#10: sales per beer size/beer price range/beer discount

#anything else ...


#table schema:
#1: sessions
sessions.schema("""
    "id", string, PRIMARY KEY
 ?  "userId", string, might be null if not registered
 ?  "city", string, might be obtained from the IP address
 ?  "province", string, might be obtained from the IP address
 ?  "country", string, might be obtained from the IP address
    "startTime", timestamp,
    "startDate", date,
    "endTime", timestamp,
    "endData", date,
    "duration", timestamp, duration = endTime - StartTime
    "sessionSource", string, web traffic source of this session
    "pageCounts", int, total page viewed during the session
    "pageIds", list of string, list of all viewed page ids during thie session
    "pageURLs", list of string, list of all pages viewed during this session
    "deviceType", string, desktop vs tablet vs mobile
    "browerType", string,
    "operationSystem", string,
""")

#2: pageviews
pageviews.schema("""
    "id", string, PRIMARY KEY
    "sessionId", string, FOREIGN KEY from the sessions table
    "startTime", timestamp,
    "endTime", timestamp,
 $  "duration", timestamp,
    "pageURL", string,
    "pageVersionId", string, for split-test purpose, can be null if no tests
    "pageType", string, classify the page type, ie checkout page vs cart page vs search page vs beer page vs vendor page
 $  "pageLevel", string, classify the page level for the goal conversion
    "pageSourceId", string, the internal source page clicked to proceed to this page, or null for external source
    "pageNextId", string, the internal page proceed next, or null if quit the session
    "eventId", string, refers to an event (subscription, clicks, carts, ...) or null if no special events happened
    "eventType", string, ie subscription, clicks, carts, checkout, ...
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")

#3: registrations
registrations.schema("""
    "id", string, PRIMARY KEY
 $  "type", string, membership type, ie exclusive, deluxe, basic or regular (not a club member)
    "name", string,
    "email", string,
    "age", int,
    "gender", string,
    "city", string,
    "province", string,
    "country", string,
    "zip", string,
    "eventTime", timestamp,
    "eventDate", date,
    "eventId", string, a key which can look up to get page & session info
    "pageId", string, the page id where this event happens
    "pageURL", string, the page url where this event happens
    "sessionId", string, the session id
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")

#4: subscriptions
subscriptions.schema("""
    "id", string, PRIMARY KEY
    "userId", string,
    "type", string, ie following a vendor, a beer or a user
    "itemId", string, the id of the item beeing followed
    "itemPosition", string, the position of the subscribed item in that page
    "ralatedItems", list of string, all items in that page
    "eventTime", timestamp, the exact time that this event happens
    "eventDate", date,
    "eventId", string, a key which can look up to get page & session info
    "pageId", string, the page id
    "pageURL", string,
    "sessionId", string, the session id
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")

#5: searches
searches.schema("""
    "id", string, PRIMARY KEY
    "userId", string,
 ?  "type", string, means the type of the searched object, ie vendor, beer or something else
    "searchTerms", string, the typed search query as as a string
    "singleSearchTerms", list of string, the list of single words split by searchTerms
 $  "action", string, 'yes' if the user click any of those results; 'no' if not
    "itemId", string, the item being clicked, or null if action = no
    "itemPosition", string, the position of the clicked item in that page, or null if action = no
    "ralatedItems", list of string, all items in that page for both action = yes and action = no
    "eventTime", timestamp, the exact time that this event happens
    "eventDate", date,
    "eventId", string,
    "pageId", string, the page id
    "pageURL", string,
    "sessionId", string, the session id
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")

#6: clicks
clicks.schema("""
    "id", string, PRIMARY KEY
    "userId", string,
 ?  "type", string, ie review, similar, regular or vendor
    "itemId", string, the item has been clicked
    "itemPosition", string, the position of the clicked item in that page
    "ralatedItems", list of string, all related items in that page
    "eventTime", timestamp, the exact time that this event happens
    "eventDate", date,
    "eventId", string,
    "pageId", string, the page id
    "pageURL", string,
    "sessionId", string, the session id
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")

#7: carts
carts.schema("""
    "id", string, PRIMARY KEY
    "userId", string,
 ?  "type", string, ie mini or full
    "beerId", string, the beer has been added or removed
    "inventoryId", string, use it to get size and some other info
    "action", string, either add or remove
    "quantity", int, can be negative if it has been removed, but can not be zero
    "currentPrice", float,
    "originalPrice", float,
    "remaining", int,
    "itemPosition", string, the position of the clicked item in that page
    "ralatedItems", list of string, all related items in that page
    "eventTime", timestamp, the exact time that this event happens
    "eventDate", date,
    "eventId", string,
    "pageId", string, the page id
    "pageURL", string,
    "sessionId", string, the session id
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")

#8: checkouts
checkouts.schema("""
    "id", string, PRIMARY KEY
    "orderId", string, refer to the corresponding order
    "userId", string,
    "beerId", string, the beer has been ordered
    "inventoryId", string,
    "quantity" int, must be positive
    "currentPrice", float,
    "originalPrice", float,
    "size", string,
    "totalItemCount", int, total different items in the cart
    "totalQuantities", int, total quantities in the cart
    "totalPrice", float, total price in the cart
    "shippingCost", float,
    "checkout", bool, true if checkout or false if not
    "eventTime", timestamp, the exact time that this event happens
    "eventDate", date,
    "pageId", string, the page id
    "pageURL", string,
    "sessionId", string, the session id
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")

#9: orders
orders.schema("""
    "id", string, PRIMARY KEY
    "userId", string,
    "beerIds", list of string, all beers have been ordered
    "inventoryIds", list of string,
    "totalItems", int
    "totalQuantity" int,
    "totalPrice", float,
    "shippingCost", float,
    "paymentMethod", string,
    "billingCity", string,
    "billingProvince", string,
    "billingCountry", string,
    "billingZip", string,
    "eventTime", timestamp, the exact time that this event happens
    "eventDate", date,
    "eventId", string,
    "pageId", string, the page id
    "pageURL", string,
    "sessionId", string, the session id
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")

#10: ratings
ratings.schema("""
    "id", string, PRIMARY KEY
    "userId", string,
    "rateType", string, beer or vendor or something else
    "itemId", string,
    "itemPosition", string, the position of the clicked item in that page
    "ralatedItems", list of string, all related items in that page
    "rate", int, scale: 0 - 5
 $  "comments", string, some possible comments
    "eventTime", timestamp, the exact time that this event happens
    "eventDate", date,
    "eventId", string,
    "pageId", string, the page id
    "pageURL", string,
    "sessionId", string, the session id
    "deviceType", string,
    "browerType", string,
    "operationSystem", string,
""")
