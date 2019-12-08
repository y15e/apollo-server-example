import * as mongodb from 'mongodb'

import express from 'express';
import cors from 'cors';
import bodyParser from 'body-parser';
import { createServer } from 'http';
import { graphqlExpress, graphiqlExpress } from 'graphql-server-express';

// Subs
import { execute, subscribe } from 'graphql'
import { SubscriptionServer } from 'subscriptions-transport-ws';

const PORT = 3020;
const SUBSCRIPTIONS_PATH = '/subscriptions';

import { makeExecutableSchema } from 'graphql-tools';

// mongo
const dbName = 'stream-test'
const mongoUrl = 'mongodb://localhost:27018/' + dbName + '?readPreference=secondaryPreferred'

const mongoOptions = {
  useNewUrlParser: true,
  useUnifiedTopology: true
}

const MongoClient = mongodb.MongoClient
const client = new MongoClient(mongoUrl, mongoOptions)

async function start() {
  try {
	
	await client.connect()
	
	const db = client.db(dbName)
	const collection = db.collection('items')
	
	const changeStreamIterator = collection.watch()

	// schema
	const typeDefs = [`
  type Tag {
    id: Int
    label: String
    type: String
  }

  type TagsPage {
    tags: [Tag]
    hasMore: Boolean
  }

  type Query {
    hello: String
    ping(message: String!): String
    tags(type: String!): [Tag]
    tagsPage(page: Int!, size: Int!): TagsPage
    randomTag: Tag
    lastTag: Tag
  }

  type Mutation {
    addTag(type: String!, label: String!): Tag
  }

  type Subscription {
    tagAdded(type: String!): Tag
  }

  schema {
    query: Query
    mutation: Mutation
    subscription: Subscription
  }
`];

	const resolvers = {
	  Query: {
		hello(root, args, context) {
		  return "Hello world!";
		},
	  },
	  Mutation: {
		addTag: async (root, { type, label }, context) => {
		  console.log(`adding ${type} tag '${label}'`);
		  const newTag = await Tags.addTag(type, label);
		  pubsub.publish(TAGS_CHANGED_TOPIC, { tagAdded: newTag });
		  return newTag;
		},
	  },
	  Subscription: {
		tagAdded: {
		  subscribe: async function * () {
			while (true) {
			  const result = await changeStreamIterator.next()
			  console.log(result.fullDocument)
			  yield {
				tagAdded: {
				  ...result.fullDocument
				}
			  }
			}
		  }
		}
	  },
	};
	
	const schema = makeExecutableSchema({
	  typeDefs,
	  resolvers,
	});
	
	// express
	var app = express();
	
	app.use(cors());
	
	app.use(bodyParser.urlencoded({ extended: true }));
	app.use(bodyParser.json());
	
	app.use('/graphql', graphqlExpress({ schema }));
	
	app.use('/graphiql', graphiqlExpress({
	  endpointURL: '/graphql',
	}));
	
	const server = createServer(app)
	
	server.listen(PORT, () => {
	  console.log(`http://localhost:${PORT}/graphql`)
	  console.log(`ws://localhost:${PORT}${SUBSCRIPTIONS_PATH}`)
	});
	
	// Subs
	SubscriptionServer.create(
	  {
		schema,
		execute,
		subscribe,
	  },
	  {
		server,
		path: SUBSCRIPTIONS_PATH,
	  }
	);
	
  } catch (err) {
	console.dir(err)
  }
}

start()
	
