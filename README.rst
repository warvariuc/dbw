``DBW`` stands for 'DataBase Wrapper'. I could not find a better name, especially as it's both an ORM
and a DAL.

``DBW`` was created as part of another project of mine which needed an ORM, but i didn't like existing
solutions.

It is a ``DAL`` (Database Abstraction Layer) **and** an ``ORM`` (Object-Relational Mapper).
The design is driven by the idea to have Python IDE autocomplete (static analysis) work.
Also humans studying the code should not scratch their heads trying to understand from where an
attribute has appeared in an object.
Django ORM has **too** much magic - you have too look into docs to know all the params.

::

    class Book(Model):
        price = DecimalField(max_digits=10, decimal_places=2, default='0.00')


In Django you write::

    Book.objects.filter(price__gte=10)

to select all books with price >= 10.

With DBW you do::

    Book.objects.select(Book.price >= 10)

In Django you do::

    Book.objects.filter(price__gte=10).values('id')

In DBW you write::

   db.select(Book.id, where=(Book.price >= 10))

No need for ``QuerySet`` to chain filters, because ``where`` expressions can also be chained and they
are, imo, more logical way for filtering objects.

In our case the code is trying to be self-explanatory. Using autocomplete and code following you can
find out all params and their meaning.


Models and database tables are named in singular.

> I stick with Singular for table names and any programming entity. The reason? The fact that there
are irregular plurals in English like mouse -> mice and sheep -> sheep.
city -> cities ?

The code is in Beta state. It needs extensive testing. It is not thread-safe. It does not have caching yet.

I decided to release the code in its current state after watching "'Good Enough' is good enough!" by
Alex Martelli at PyCon 2013: Launch early, launch often!
