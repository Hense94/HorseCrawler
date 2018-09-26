# HorseCrawler


# TODO:
# - Deferererer visiting roberts untill we make a request to the page
# 
#   /
# \/- Undo force http on roberts.txt
# 
#   /
# \/ store where things link #PivotTables
# 
#   /
# \/ SSL_Cert fejl (gotta catch em all)

``` 
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;
```