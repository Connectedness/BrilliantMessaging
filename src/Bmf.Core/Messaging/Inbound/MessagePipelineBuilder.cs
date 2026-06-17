using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Builds the inbound message pipeline by composing middleware around a terminal handler, mirroring the
/// ASP.NET Core request-pipeline model: components added first run first (outermost) and wrap those added later.
/// </summary>
public sealed class MessagePipelineBuilder
{
    private readonly List<Func<MessageDelegate, MessageDelegate>> _components = [];

    /// <summary>
    /// Adds a middleware component expressed as a function that wraps the next <see cref="MessageDelegate" />.
    /// </summary>
    /// <param name="middleware">A factory that, given the next delegate, returns the delegate for this stage.</param>
    /// <returns>The same <see cref="MessagePipelineBuilder" /> instance for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="middleware" /> is <see langword="null" />.</exception>
    public MessagePipelineBuilder Use(Func<MessageDelegate, MessageDelegate> middleware)
    {
        _components.Add(middleware ?? throw new ArgumentNullException(nameof(middleware)));
        return this;
    }

    /// <summary>
    /// Adds an inline middleware component that receives the current context and the next delegate, and decides
    /// whether and when to invoke it.
    /// </summary>
    /// <param name="middleware">The inline middleware to add.</param>
    /// <returns>The same <see cref="MessagePipelineBuilder" /> instance for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="middleware" /> is <see langword="null" />.</exception>
    public MessagePipelineBuilder Use(Func<IncomingMessageContext, MessageDelegate, Task> middleware)
    {
        if (middleware is null)
        {
            throw new ArgumentNullException(nameof(middleware));
        }

        return Use(next => context => middleware(context, next));
    }

    /// <summary>
    /// Adds a middleware component of type <typeparamref name="TMiddleware" />, resolved from the per-message
    /// service provider on each invocation.
    /// </summary>
    /// <typeparam name="TMiddleware">The middleware type to resolve and invoke.</typeparam>
    /// <returns>The same <see cref="MessagePipelineBuilder" /> instance for chaining.</returns>
    public MessagePipelineBuilder UseMiddleware<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware
    {
        return Use(
            next => async context =>
            {
                var middleware = context.Services.GetRequiredService<TMiddleware>();
                await middleware.InvokeAsync(context, next).ConfigureAwait(false);
            }
        );
    }

    /// <summary>
    /// Composes the registered middleware around the given terminal delegate and returns the entry delegate of
    /// the assembled pipeline.
    /// </summary>
    /// <param name="terminal">The innermost delegate invoked after all middleware has run (typically the handler dispatch).</param>
    /// <returns>The composed <see cref="MessageDelegate" /> representing the full pipeline.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="terminal" /> is <see langword="null" />.</exception>
    public MessageDelegate Build(MessageDelegate terminal)
    {
        if (terminal is null)
        {
            throw new ArgumentNullException(nameof(terminal));
        }

        var app = terminal;

        for (var i = _components.Count - 1; i >= 0; i--)
        {
            app = _components[i](app);
        }

        return app;
    }
}
