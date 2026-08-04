import pg from "pg"
pg.types.setTypeParser(20, BigInt)
