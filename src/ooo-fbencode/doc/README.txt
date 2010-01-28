Simple OpenOffice.org Calc extension which provides two new functions:

 FBKEYENCODE(string) - takes a Wikipedia article name and 
                       returns a key encoded the way Freebase wants to see it.
                       Prepend /wikipedia/en (or other language namespace) to
                       create a key which can be used to lookup the topic.

 JSONENCODE(string) - encode given string using json.JSONEncoder().encode()

Much thanks to Jan Holst Jensen (jan at biochemfusion.com) for providing 
a complete working example in his DoobieDoo plugin tutorial.  Building this
based on the OpenOffice.org documentation would have been impossible.
