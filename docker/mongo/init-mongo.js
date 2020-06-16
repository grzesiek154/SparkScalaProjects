db.createUser(
    {
        user : "root",
        pwd : "root",
        roles : [
            {
                role : "readWrite",
                db : "your-database-name"
            }
        ]
    }
)