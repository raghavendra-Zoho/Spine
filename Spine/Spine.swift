//
//  Resource.swift
//  Spine
//
//  Created by Ward van Teijlingen on 21-08-14.
//  Copyright (c) 2014 Ward van Teijlingen. All rights reserved.
//

import Foundation
import BrightFutures

public typealias Metadata = [String: Any]
public typealias JSONAPIData = [String: Any]

/// The main class
open class Spine {

	/// The router that builds the URLs for requests.
	let router: Router

	/// The HTTPClient that performs the HTTP requests.
	open let networkClient: NetworkClient

	/// The serializer to use for serializing and deserializing of JSON representations.
	open let serializer: Serializer = Serializer()

	/// The key formatter to use for formatting field names to keys.
	open var keyFormatter: KeyFormatter = DasherizedKeyFormatter() {
		didSet {
			router.keyFormatter = keyFormatter
			serializer.keyFormatter = keyFormatter
		}
	}

	/// The operation queue on which all operations are queued.
	let operationQueue = OperationQueue()


	// MARK: Initializers

	/**
	Creates a new Spine instance using the given router and network client.
	*/
	public init(router: Router, networkClient: NetworkClient) {
		self.router = router
		self.networkClient = networkClient
		self.operationQueue.name = "com.wardvanteijlingen.spine"

		self.router.keyFormatter = keyFormatter
		self.serializer.keyFormatter = keyFormatter
	}

	/**
	Creates a new Spine instance using the default Router and HTTPClient classes.
	*/
	public convenience init(baseURL: URL) {
		let router = JSONAPIRouter()
		router.baseURL = baseURL
		self.init(router: router, networkClient: HTTPClient())
	}

	/**
	Creates a new Spine instance using a specific router and the default HTTPClient class.
	Use this initializer to specify a custom router.
	*/
	public convenience init(router: Router) {
		self.init(router: router, networkClient: HTTPClient())
	}

	/**
	Creates a new Spine instance using a specific network client and the default Router class.
	Use this initializer to specify a custom network client.
	*/
	public convenience init(baseURL: URL, networkClient: NetworkClient) {
		let router = JSONAPIRouter()
		router.baseURL = baseURL
		self.init(router: router, networkClient: networkClient)
	}


	// MARK: Operations

	/**
	Adds the given operation to the operation queue.
	This sets the spine property of the operation to this Spine instance.

	- parameter operation: The operation to enqueue.
	*/
	func addOperation(_ operation: ConcurrentOperation) {
		operation.spine = self
		operationQueue.addOperation(operation)
	}


	// MARK: Fetching

	/**
	Fetch multiple resources using the given query.

	- parameter query: The query describing which resources to fetch.

	- returns: A future that resolves to a tuple containing the fetched ResourceCollection, the document meta, and the document jsonapi object.
	*/
	open func find<T: Resource>(_ query: Query<T>) -> Future<(resources: ResourceCollection, meta: Metadata?, jsonapi: JSONAPIData?), SpineError> {
		let promise = Promise<(resources: ResourceCollection, meta: Metadata?, jsonapi: JSONAPIData?), SpineError>()

		let operation = FetchOperation(query: query, spine: self)

		operation.completionBlock = { [unowned operation] in

			switch operation.result! {
			case .success(let document):
				let response = (ResourceCollection(document: document), document.meta, document.jsonapi)
				promise.success(response)
			case .failure(let error):
				promise.failure(error)
			}
		}

		addOperation(operation)

		return promise.future
	}

	/**
	Fetch multiple resources with the given IDs and type.

	- parameter IDs:  Array containing IDs of resources to fetch.
	- parameter type: The type of resource to fetch.

	- returns: A future that resolves to a tuple containing the fetched ResourceCollection, the document meta, and the document jsonapi object.
	*/
	open func find<T: Resource>(_ ids: [String], ofType type: T.Type) -> Future<(resources: ResourceCollection, meta: Metadata?, jsonapi: JSONAPIData?), SpineError> {
		let query = Query(resourceType: type, resourceIDs: ids)
		return find(query)
	}

	/**
	Fetch one resource using the given query.
	If the response contains multiple resources, the first resource is returned.
	If the response indicates success but doesn't contain any resources, the returned future fails.

	- parameter query: The query describing which resource to fetch.

	- returns: A future that resolves to a tuple containing the fetched resource, the document meta, and the document jsonapi object.
	*/
	open func findOne<T: Resource>(_ query: Query<T>) -> Future<(resource: T, meta: Metadata?, jsonapi: JSONAPIData?), SpineError> {
		let promise = Promise<(resource: T, meta: Metadata?, jsonapi: JSONAPIData?), SpineError>()

		let operation = FetchOperation(query: query, spine: self)

		operation.completionBlock = { [unowned operation] in
			switch operation.result! {
			case .success(let document) where document.data?.count == 0 || document.data == nil:
				promise.failure(SpineError.resourceNotFound)
			case .success(let document):
				let firstResource = document.data!.first as! T
				let response = (resource: firstResource, meta: document.meta, jsonapi: document.jsonapi)
				promise.success(response)
			case .failure(let error):
				promise.failure(error)
			}
		}

		addOperation(operation)

		return promise.future
	}

	/**
	Fetch one resource with the given ID and type.
	If the response contains multiple resources, the first resource is returned.
	If the response indicates success but doesn't contain any resources, the returned future fails.

	- parameter id:   ID of resource to fetch.
	- parameter type: The type of resource to fetch.

	- returns: A future that resolves to a tuple containing the fetched resource, the document meta, and the document jsonapi object.
	*/
	open func findOne<T: Resource>(_ id: String, ofType type: T.Type) -> Future<(resource: T, meta: Metadata?, jsonapi: JSONAPIData?), SpineError> {
		let query = Query(resourceType: type, resourceIDs: [id])
		return findOne(query)
	}

	/**
	Fetch all resources with the given type.
	This does not explicitly impose any limit, but the server may choose to limit the response.

	- parameter type: The type of resource to fetch.

	- returns: A future that resolves to a tuple containing the fetched ResourceCollection, the document meta, and the document jsonapi object.
	*/
	open func findAll<T: Resource>(_ type: T.Type) -> Future<(resources: ResourceCollection, meta: Metadata?, jsonapi: JSONAPIData?), SpineError> {
		let query = Query(resourceType: type)
		return find(query)
	}


	// MARK: Loading

	/**
	Load the given resource if needed. If its `isLoaded` property is true, it returns the resource as is.
	Otherwise it loads the resource using the passed query.

	The `queryCallback` parameter can be used if the resource should be loaded by using a custom query.
	For example, in case you want to include a relationship or load a sparse fieldset.

	Spine creates a basic query to load the resource and passes it to the queryCallback.
	From this callback you return a modified query, or a whole new query if desired. This returned
	query is then used when loading the resource.

	- parameter resource:      The resource to ensure.
	- parameter queryCallback: A optional function that returns the query used to load the resource.

	- returns: A future that resolves to the loaded resource.
	*/
	open func load<T: Resource>(_ resource: T, queryCallback: ((Query<T>) -> Query<T>)? = nil) -> Future<T, SpineError> {
		var query = Query(resource: resource)
		if let callback = queryCallback {
			query = callback(query)
		}
		return loadResourceByExecutingQuery(resource, query: query)
	}

	/**
	Reload the given resource even when it's already loaded. The returned resource will be the same
	instance as the passed resource.

	The `queryCallback` parameter can be used if the resource should be loaded by using a custom query.
	For example, in case you want to include a relationship or load a sparse fieldset.

	Spine creates a basic query to load the resource and passes it to the queryCallback.
	From this callback you return a modified query, or a whole new query if desired. This returned
	query is then used when loading the resource.

	- parameter resource:      The resource to reload.
	- parameter queryCallback: A optional function that returns the query used to load the resource.

	- returns: A future that resolves to the reloaded resource.
	*/
	open func reload<T: Resource>(_ resource: T, queryCallback: ((Query<T>) -> Query<T>)? = nil) -> Future<T, SpineError> {
		var query = Query(resource: resource)
		if let callback = queryCallback {
			query = callback(query)
		}
		return loadResourceByExecutingQuery(resource, query: query, skipIfLoaded: false)
	}

	func loadResourceByExecutingQuery<T: Resource>(_ resource: T, query: Query<T>, skipIfLoaded: Bool = true) -> Future<T, SpineError> {
		let promise = Promise<T, SpineError>()

		if skipIfLoaded && resource.isLoaded {
			promise.success(resource)
			return promise.future
		}

		let operation = FetchOperation(query: query, spine: self)
		operation.mappingTargets = [resource]
		operation.completionBlock = { [unowned operation] in
			if let error = operation.result?.error {
				promise.failure(error)
			} else {
				promise.success(resource)
			}
		}

		addOperation(operation)

		return promise.future
	}


	// MARK: Paginating

	/**
	Loads the next page of the given resource collection. The newly loaded resources are appended to the passed collection.
	When the next page is not available, the returned future will fail with a `NextPageNotAvailable` error code.

	- parameter collection: The collection for which to load the next page.

	- returns: A future that resolves to the ResourceCollection including the newly loaded resources.
	*/
	open func loadNextPageOfCollection(_ collection: ResourceCollection) -> Future<ResourceCollection, SpineError> {
		let promise = Promise<ResourceCollection, SpineError>()

		if let nextURL = collection.nextURL {
			let query = Query(url: nextURL)
			let operation = FetchOperation(query: query, spine: self)

			operation.completionBlock = { [unowned operation] in
				switch operation.result! {
				case .success(let document):
					let nextCollection = ResourceCollection(document: document)
					collection.resources += nextCollection.resources
					collection.resourcesURL = nextCollection.resourcesURL
					collection.nextURL = nextCollection.nextURL
					collection.previousURL = nextCollection.previousURL

					promise.success(collection)
				case .failure(let error):
					promise.failure(error)
				}
			}

			addOperation(operation)

		} else {
			promise.failure(SpineError.nextPageNotAvailable)
		}

		return promise.future
	}

	/**
	Loads the previous page of the given resource collection. The newly loaded resources are prepended to the passed collection.
	When the previous page is not available, the returned future will fail with a `PreviousPageNotAvailable` error code.

	- parameter collection: The collection for which to load the previous page.

	- returns: A future that resolves to the ResourceCollection including the newly loaded resources.
	*/
	open func loadPreviousPageOfCollection(_ collection: ResourceCollection) -> Future<ResourceCollection, SpineError> {
		let promise = Promise<ResourceCollection, SpineError>()

		if let previousURL = collection.previousURL {
			let query = Query(url: previousURL)
			let operation = FetchOperation(query: query, spine: self)

			operation.completionBlock = { [unowned operation] in
				switch operation.result! {
				case .success(let document):
					let previousCollection = ResourceCollection(document: document)
					collection.resources = previousCollection.resources + collection.resources
					collection.resourcesURL = previousCollection.resourcesURL
					collection.nextURL = previousCollection.nextURL
					collection.previousURL = previousCollection.previousURL

					promise.success(collection)
				case .failure(let error):
					promise.failure(error)
				}
			}

			addOperation(operation)

		} else {
			promise.failure(SpineError.previousPageNotAvailable)
		}

		return promise.future
	}


	// MARK: Persisting

	/**
	Saves the given resource.

	- parameter resource: The resource to save.

	- returns: A future that resolves to the saved resource.
	*/
	open func save<T: Resource>(_ resource: T) -> Future<T, SpineError> {
		let promise = Promise<T, SpineError>()
		let operation = SaveOperation(resource: resource, spine: self)

		operation.completionBlock = { [unowned operation] in
			if let error = operation.result?.error {
				promise.failure(error)
			} else {
				promise.success(resource)
			}
		}

		addOperation(operation)

		return promise.future
	}
	open func copyAll<T: Resource>(_ resources: [T], _ query : Query<T>) -> Future<(resources: ResourceCollection, meta: Metadata?, jsonapi: JSONAPIData?), SpineError> {
        let promise = Promise<(resources: ResourceCollection, meta: Metadata?, jsonapi: JSONAPIData?), SpineError>()
        let operation = CopyOperation(resources: resources, spine: self, query: query)
        operation.completionBlock = { [unowned operation] in
            switch operation.result! {
            case .success(let document):
                let response = (ResourceCollection(document: document), document.meta, document.jsonapi)
                promise.success(response)
            case .failure(let error):
                promise.failure(error)
            }
        }
        addOperation(operation)
        return promise.future
    }
	open func saveAll<T: Resource>(_ resources: [T]) -> Future<(resources: ResourceCollection, meta: Metadata?, jsonapi: JSONAPIData?), SpineError> {
        let promise = Promise<(resources: ResourceCollection, meta: Metadata?, jsonapi: JSONAPIData?), SpineError>()
        let operation = SaveOperation(resources: resources, spine: self)
        operation.completionBlock = { [unowned operation] in
            switch operation.result! {
            case .success(let document):
                let response = (ResourceCollection(document: document), document.meta, document.jsonapi)
                promise.success(response)
            case .failure(let error):
                promise.failure(error)
            }
        }
        addOperation(operation)
        return promise.future
    }
	/**
	Deletes the given resource.

	- parameter resource: The resource to delete.

	- returns: A future
	*/
	open func delete<T: Resource>(_ resource: T) -> Future<Void, SpineError> {
		let promise = Promise<Void, SpineError>()
		let operation = DeleteOperation(resource: resource, spine: self)

		operation.completionBlock = { [unowned operation] in
			if let error = operation.result?.error {
				promise.failure(error)
			} else {
				promise.success()
			}
		}

		addOperation(operation)

		return promise.future
	}
}

/**
Extension regarding registration.
*/
public extension Spine {
	/**
	Registers a resource class.

	- parameter resourceClass: The resource class to register.
	*/
	func registerResource(_ resourceClass: Resource.Type) {
		serializer.registerResource(resourceClass)
	}

	/**
	Registers transformer `transformer`.

	- parameter transformer: The Transformer to register.
	*/
	func registerValueFormatter<T: ValueFormatter>(_ formatter: T) {
		serializer.registerValueFormatter(formatter)
	}
}


// MARK: - Failable

/**
Represents the result of a failable operation.

- Success: The operation succeeded with the given result.
- Failure: The operation failed with the given error.
*/
enum Failable<T, E: Error> {
	case success(T)
	case failure(E)

	init(_ value: T) {
		self = .success(value)
	}

	init(_ error: E) {
		self = .failure(error)
	}

	var error: E? {
		switch self {
		case .failure(let error):
			return error
		default:
			return nil
		}
	}
}
