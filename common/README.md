# Common files

> What's the difference between helper and utility functions? What kind of function should I put in each file?

In software engineering, the terms "helper functions" and "utility functions" are often used interchangeably, but they can have nuanced differences based on their intended use. Here's a breakdown:

## Utility Functions

1. **General-Purpose**: These are general-purpose functions that perform common tasks unrelated to the business logic. They are often stateless and take inputs to return outputs without any side effects.
  
2. **Reusable Across Projects**: Utility functions are typically so general that they can be reused not just across multiple modules in a single project but also across multiple projects.

3. **Examples**: String manipulation functions, date format converters, or mathematical calculations.

## Helper Functions

1. **Specific-Purpose**: Helper functions are generally specific to your project or business logic. They encapsulate a sequence of operations that are frequently used within a more complex method to make it more readable or maintainable.

2. **Tied to Business Logic**: These are often customized for the particular needs of your application and might not be as reusable as utility functions.

3. **Examples**: A function that validates if a user has the correct permissions to perform a certain action in your specific application.

## Where to Put Which Function

1. **Utility Functions**: Place general-purpose functions that are likely to be reused across projects in a `utils.py` file.

2. **Helper Functions**: Place functions that are specific to your project's business logic but are shared across multiple modules in a `helpers.py` file.

By separating these, you make it easier to identify the purpose of each function, which can be particularly useful in larger projects or when working in a team.
