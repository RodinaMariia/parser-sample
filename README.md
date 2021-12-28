## Selenium-based parser.

Simple parser sample for the zakupki.gov site. The application works with three page types: planned or finished auctions, concluded contracts and organizations, both of buyers and sellers. 

Primarily its necessary to get the whole urls we are going to process with the *create_urls* function provides searching by raw html queries from the target site. It’s important to use slicing parameter working with a long period of time because the original API has a limit on the number of pages in search result.  The next step is to iterate over found pages with initialized by page type parser. It’s possible to implement a parser to the new page type inherited from the *BasicParser* class.

Collected information keeps at the outer storage. A custom storage has to support the *StorageAdapter* interface, presently there are two implementations available: pandas *DataFrame*-based and SQLite-based.

Some parts of the code have been removed.

**For reference only.**
