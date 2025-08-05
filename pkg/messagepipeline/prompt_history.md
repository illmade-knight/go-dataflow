## The refactor

We've realized making this package generic was a mistake.

The messagepipeline's purpose is to add metadata or transform raw message payloads

The existing package does this in an overcomplex way.

### Desired outcome

The pipeline transformers should take a PipelineMessage
* act on the raw payload 
* adjust the EnrichmentData
* allow no unmarshall passing of Attribute data 

We want to remove all generics:

our idea now is to change type MessageTransformer[T any] func(ctx context.Context, msg PipelineMessage) (payload *T, skip bool, err error)

to instead modify the PipelineMessage in place:

MessageTransformer func(ctx context.Context, msg *PipelineMessage) (skip bool, err error)


### Evaluate and plan

can you evaluate this refactoring and outline a plan to accomplise it