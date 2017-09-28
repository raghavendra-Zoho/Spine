//
//  Operation.swift
//  Spine
//
//  Created by Ward van Teijlingen on 05-04-15.
//  Copyright (c) 2015 Ward van Teijlingen. All rights reserved.
//

import Foundation

private func statusCodeIsSuccess(_ statusCode: Int?) -> Bool {
	return statusCode != nil && 200 ... 299 ~= statusCode!
}

private func errorFromStatusCode(_ statusCode: Int, additionalErrors: [APIError]? = nil) -> SpineError {
	return SpineError.serverError(statusCode: statusCode, apiErrors: additionalErrors)
}

///  Promotes an ErrorType to a higher level SpineError.
///  Errors that cannot be represented as a SpineError will be returned as SpineError.UnknownError
private func promoteToSpineError(_ error: Error) -> SpineError {
	switch error {
	case let error as SpineError:
		return error
	case is SerializerError:
		return .serializerError
	default:
		return .unknownError
	}
}

// MARK: - Base operation

/**
The ConcurrentOperation class is an abstract class for all Spine operations.
You must not create instances of this class directly, but instead create
an instance of one of its concrete subclasses.

Subclassing
===========
To support generic subclasses, Operation adds an `execute` method.
Override this method to provide the implementation for a concurrent subclass.

Concurrent state
================
ConcurrentOperation is concurrent by default. To update the state of the operation,
update the `state` instance variable. This will fire off the needed KVO notifications.

Operating against a Spine
=========================
The `Spine` instance variable references the Spine against which to operate.
*/
class ConcurrentOperation: Operation {
	enum State: String {
		case Ready = "isReady"
		case Executing = "isExecuting"
		case Finished = "isFinished"
	}
	
	/// The current state of the operation
	var state: State = .Ready {
		willSet {
			willChangeValue(forKey: newValue.rawValue)
			willChangeValue(forKey: state.rawValue)
		}
		didSet {
			didChangeValue(forKey: oldValue.rawValue)
			didChangeValue(forKey: state.rawValue)
		}
	}
	override var isReady: Bool {
		return super.isReady && state == .Ready
	}
	override var isExecuting: Bool {
		return state == .Executing
	}
	override var isFinished: Bool {
		return state == .Finished
	}
	override var isAsynchronous: Bool {
		return true
	}
	
	/// The Spine instance to operate against.
	var spine: Spine!
	
	/// Convenience variables that proxy to their spine counterpart
	var router: Router {
		return spine.router
	}
	var networkClient: NetworkClient {
		return spine.networkClient
	}
	var serializer: Serializer {
		return spine.serializer
	}
	
	override init() {}
	
	final override func start() {
		if isCancelled {
			state = .Finished
		} else {
			state = .Executing
			main()
		}
	}
	
	final override func main() {
		execute()
	}
	
	func execute() {}
}


// MARK: - Main operations

/// FetchOperation fetches a JSONAPI document from a Spine, using a given Query.
class FetchOperation<T: Resource>: ConcurrentOperation {
	/// The query describing which resources to fetch.
	let query: Query<T>
	
	/// Existing resources onto which to map the fetched resources.
	var mappingTargets = [Resource]()
	
	/// The result of the operation. You can safely force unwrap this in the completionBlock.
	var result: Failable<JSONAPIDocument, SpineError>?
	
	init(query: Query<T>, spine: Spine) {
		self.query = query
		super.init()
		self.spine = spine
	}
	
	override func execute() {
		let url = spine.router.urlForQuery(query)
		
		Spine.logInfo(.spine, "Fetching document using URL: \(url)")
		
		networkClient.request(method: "GET", url: url) { statusCode, responseData, networkError in
			defer { self.state = .Finished }
			
			guard networkError == nil else {
				self.result = .failure(SpineError.networkError(networkError!))
				return
			}
			
			if let data = responseData , data.count > 0 {
				do {
					let document = try self.serializer.deserializeData(data, mappingTargets: self.mappingTargets)
					if statusCodeIsSuccess(statusCode) {
						self.result = Failable(document)
					} else {
						self.result = .failure(SpineError.serverError(statusCode: statusCode!, apiErrors: document.errors))
					}
				} catch let error {
					self.result = .failure(promoteToSpineError(error))
				}
				
			} else {
				self.result = .failure(errorFromStatusCode(statusCode!))
			}
		}
	}
}

/// DeleteOperation deletes a resource from a Spine.
class DeleteOperation: ConcurrentOperation {
	/// The resource to delete.
	let resource: Resource
	
	/// The result of the operation. You can safely force unwrap this in the completionBlock.
	var result: Failable<Void, SpineError>?
	
	init(resource: Resource, spine: Spine) {
		self.resource = resource
		super.init()
		self.spine = spine
	}
	
	override func execute() {
		let URL = spine.router.urlForQuery(Query(resource: resource))
		
		Spine.logInfo(.spine, "Deleting resource \(resource) using URL: \(URL)")
		
		networkClient.request(method: "DELETE", url: URL) { statusCode, responseData, networkError in
			defer { self.state = .Finished }
		
			guard networkError == nil else {
				self.result = Failable.failure(SpineError.networkError(networkError!))
				return
			}
			
			if statusCodeIsSuccess(statusCode) {
				self.result = Failable.success()
			} else if let data = responseData , data.count > 0 {
				do {
					let document = try self.serializer.deserializeData(data, mappingTargets: nil)
					self.result = .failure(SpineError.serverError(statusCode: statusCode!, apiErrors: document.errors))
				} catch let error {
					self.result = .failure(promoteToSpineError(error))
				}
			} else {
				self.result = .failure(errorFromStatusCode(statusCode!))
			}
		}
	}
}

/// A SaveOperation updates or adds a resource in a Spine.
open func urlForQuery<T: Resource>(_ query: Query<T>) -> URL {
        let url: URL
        let preBuiltURL: Bool
        
        // Base URL
        if let urlString = query.url?.absoluteString {
            url = URL(string: urlString, relativeTo: baseURL)!
            preBuiltURL = true
        } else if let type = query.resourceType {
            url = urlForResourceType(type)
            preBuiltURL = false
        } else {
            preconditionFailure("Cannot build URL for query. Query does not have a URL, nor a resource type.")
        }
        
        var urlComponents = URLComponents(url: url, resolvingAgainstBaseURL: true)!
        var queryItems: [URLQueryItem] = urlComponents.queryItems ?? []
        
        // Resource IDs
        if !preBuiltURL {
            if let ids = query.resourceIDs {
                if ids.count == 1 {
                    urlComponents.path = (urlComponents.path as NSString).appendingPathComponent(ids.first!)
                } else {
                    let item = URLQueryItem(name: "filter[id]", value: ids.joined(separator: ","))
                    appendQueryItem(item, to: &queryItems)
                }
            }
        }
        
        // Includes
        if !query.includes.isEmpty {
            var resolvedIncludes = [String]()
            
            for include in query.includes {
                var keys = [String]()
                
                var relatedResourceType: Resource.Type = T.self
                for part in include.components(separatedBy: ".") {
                    if let relationship = relatedResourceType.field(named: part) as? Relationship {
                        keys.append(keyFormatter.format(relationship))
                        relatedResourceType = relationship.linkedType
                    }
                }
                
                resolvedIncludes.append(keys.joined(separator: "."))
            }
            
            let item = URLQueryItem(name: "include", value: resolvedIncludes.joined(separator: ","))
            appendQueryItem(item, to: &queryItems)
        }
        
        // Filters
        for filter in query.filters {
            let fieldName = filter.leftExpression.keyPath
            var item: URLQueryItem?
            if let field = T.field(named: fieldName) {
                item = queryItemForFilter(on: keyFormatter.format(field), value: filter.rightExpression.constantValue, operatorType: filter.predicateOperatorType)
            } else {
                item = queryItemForFilter(on: fieldName, value: filter.rightExpression.constantValue, operatorType: filter.predicateOperatorType)
            }
            appendQueryItem(item!, to: &queryItems)
        }
        
        // Fields
        for (resourceType, fields) in query.fields {
            let keys = fields.map { fieldName in
                return keyFormatter.format(T.field(named: fieldName)!)
            }
            let item = URLQueryItem(name: "fields[\(resourceType)]", value: keys.joined(separator: ","))
            appendQueryItem(item, to: &queryItems)
        }
        
        // Sorting
        if !query.sortDescriptors.isEmpty {
            let descriptorStrings = query.sortDescriptors.map { descriptor -> String in
                //let field = descriptor.key;//T.field(named: descriptor.key!)
                let key = descriptor.key; //self.keyFormatter.format(field!)
                if descriptor.ascending {
                    return key!
                } else {
                    return "-\(key!)"
                }
            }
            
            let item = URLQueryItem(name: "sort", value: descriptorStrings.joined(separator: ","))
            appendQueryItem(item, to: &queryItems)
        }
        
        // Pagination
        if let pagination = query.pagination {
            for item in queryItemsForPagination(pagination) {
                appendQueryItem(item, to: &queryItems)
            }
        }
        
        // Compose URL
        if !queryItems.isEmpty {
            urlComponents.queryItems = queryItems
        }
        
        return urlComponents.url!
    }

private class RelationshipOperation: ConcurrentOperation {
	var result: Failable<Void, SpineError>?
	
	func handleNetworkResponse(_ statusCode: Int?, responseData: Data?, networkError: NSError?) {
		defer { self.state = .Finished }
		
		guard networkError == nil else {
			self.result = Failable.failure(SpineError.networkError(networkError!))
			return
		}
		
		if statusCodeIsSuccess(statusCode) {
			self.result = Failable.success()
		} else if let data = responseData , data.count > 0 {
			do {
				let document = try serializer.deserializeData(data, mappingTargets: nil)
				self.result = .failure(errorFromStatusCode(statusCode!, additionalErrors: document.errors))
			} catch let error as SpineError {
				self.result = .failure(error)
			} catch {
				self.result = .failure(SpineError.serializerError)
			}
		} else {
			self.result = .failure(errorFromStatusCode(statusCode!))
		}
	}
}

/// A RelationshipReplaceOperation replaces the entire contents of a relationship.
private class RelationshipReplaceOperation: RelationshipOperation {
	let resource: Resource
	let relationship: Relationship

	init(resource: Resource, relationship: Relationship, spine: Spine) {
		self.resource = resource
		self.relationship = relationship
		super.init()
		self.spine = spine
	}
	
	override func execute() {
		let url = router.urlForRelationship(relationship, ofResource: resource)
		let payload: Data
		
		switch relationship {
		case is ToOneRelationship:
			payload = try! serializer.serializeLinkData(resource.value(forField: relationship.name) as? Resource)
		case is ToManyRelationship:
			let relatedResources = (resource.value(forField: relationship.name) as? ResourceCollection)?.resources ?? []
			payload = try! serializer.serializeLinkData(relatedResources)
		default:
			assertionFailure("Cannot only replace relationship contents for ToOneRelationship and ToManyRelationship")
			return
		}

		Spine.logInfo(.spine, "Replacing relationship \(relationship) using URL: \(url)")
		networkClient.request(method: "PATCH", url: url, payload: payload, callback: handleNetworkResponse)
	}
}

/// A RelationshipMutateOperation mutates a to-many relationship by adding or removing linked resources.
private class RelationshipMutateOperation: RelationshipOperation {
	enum Mutation {
		case add, remove
	}
	
	let resource: Resource
	let relationship: ToManyRelationship
	let mutation: Mutation

	init(resource: Resource, relationship: ToManyRelationship, mutation: Mutation, spine: Spine) {
		self.resource = resource
		self.relationship = relationship
		self.mutation = mutation
		super.init()
		self.spine = spine
	}

	override func execute() {
		let resourceCollection = resource.value(forField: relationship.name) as! LinkedResourceCollection
		let httpMethod: String
		let relatedResources: [Resource]
		
		switch mutation {
		case .add:
			httpMethod = "POST"
			relatedResources = resourceCollection.addedResources
		case .remove:
			httpMethod = "DELETE"
			relatedResources = resourceCollection.removedResources
		}
		
		guard !relatedResources.isEmpty else {
			result = Failable()
			state = .Finished
			return
		}
		
		let url = router.urlForRelationship(relationship, ofResource: resource)
		let payload = try! serializer.serializeLinkData(relatedResources)
		Spine.logInfo(.spine, "Mutating relationship \(relationship) using URL: \(url)")
		networkClient.request(method: httpMethod, url: url, payload: payload, callback: handleNetworkResponse)
	}
}
