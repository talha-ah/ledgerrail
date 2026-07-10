import dotenv from "dotenv"
dotenv.config()

import { connectDB } from "./db.js"
import { migrations } from "../migrations/index.js"

const PORT = parseInt(process.env.PORT ?? "3000")

// Start Server
;(async () => {
  const dbClient = await connectDB()
  await migrations(dbClient)
})()
