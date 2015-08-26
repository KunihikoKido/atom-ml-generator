# ml-generator package

Creating the data source for Amazon Machine Learning from elasticsearch.

You can create the context based categorization for multiclass classification data source.


## Commands

* Ml Generator: Update Statistics Target Terms
* Ml Generator: Create Ml Data Source


## Tips

### How to the contents of the file to random
 Amazon Machine Learning used 70% of the data for training and 30% to evaluate the model based on the defaults.

```bash
  $ (head -n +1 sample.csv && tail -n +2 sample.csv | sort -R) > sorted_sample.csv
```

> Equivalent of gnu `sort -R` on OSX?
>
> See http://superuser.com/questions/334450/equivalent-of-gnu-sort-r-on-osx
