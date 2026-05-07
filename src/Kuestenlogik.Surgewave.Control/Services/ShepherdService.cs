using Kuestenlogik.Surgewave.Control.Models;
using Microsoft.JSInterop;

namespace Kuestenlogik.Surgewave.Control.Services;

public sealed class ShepherdService(IJSRuntime js)
{
    public async Task StartTourAsync(ShepherdTourDefinition tour)
    {
        await js.InvokeVoidAsync("surgewaveShepherd.startTour", tour);
    }

    public async Task<bool> IsTourCompletedAsync(string tourId)
    {
        return await js.InvokeAsync<bool>("surgewaveShepherd.isTourCompleted", tourId);
    }

    public async Task ResetTourProgressAsync()
    {
        await js.InvokeVoidAsync("surgewaveShepherd.resetTourProgress");
    }
}
