# Wine Deep Learning

After watching [Somm](http://www.imdb.com/title/tt2204371/) (a documentary on master sommeliers) I wondered how I could create a predictive model to identify wines through blind tasting like a master sommelier. My overall goal is to create a model that can identify the variety, winery, and location of a wine based on a description that a sommelier could give after tasting a wine. Another fun future project would be to give wine recommendations based on food dishes. If anyone has any ideas or insights please share them.

---

### WineEnthusiast review data

As a first step to creating my sommelier model was gathering some data. I started by scraping ~150k wine reviews from [WineEnthusiast](http://www.winemag.com/?s=&drink_type=wine). 

The data consists of 10 fields:

- *Points*: the number of points WineEnthusiast rated the wine on a scale of 1-100 (though they say they only post reviews for wines that score >=80)
- *Variety*: the type of grapes used to make the wine (ie Pinot Noir)
- *Description*: a few sentences from a sommelier describing the wine's taste, smell, look, feel, etc.
- *Country*: the country that the wine is from
- *Province*: the province or state that the wine is from
- *Region 1*: the wine growing area in a province or state (ie Napa)
- *Region 2*: sometimes there are more specific regions specified within a wine growing area (ie Rutherford inside the Napa Valley), but this value can sometimes be blank
- *Winery*: the winery that made the wine
- *Designation*: the vineyard within the winery where the grapes that made the wine are from
- *Price*: the cost for a bottle of the wine 

I did not include the dataset that I scraped in this repository because of size, but feel free to run the scraper on your own or use the dataset that I provided on [Kaggle](https://www.kaggle.com/zynicide/wine-reviews).
