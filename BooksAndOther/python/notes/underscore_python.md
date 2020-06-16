`__foo__`: this is just a convention, a way for the Python system to use names that won't conflict with user names.

`_foo`: this is just a convention, a way for the programmer to indicate that the variable is private (whatever that means in Python).

`__foo`: this has real meaning: the interpreter replaces this name with `_classname__foo` as a way to ensure that the name will not overlap with a similar name in another class.