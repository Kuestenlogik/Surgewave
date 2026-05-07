(function() {
    'use strict';

    document.addEventListener('DOMContentLoaded', function() {
        // Compute tour file path from current page URL
        var path = window.location.pathname
            .replace(/^\//, '')
            .replace(/\/$/, '')
            .replace(/\.html$/, '')
            .replace(/\//g, '-');

        if (!path) path = 'index';

        var tourUrl = (window.__shepherd_base || '') + 'tours/' + path + '.tour.json';

        fetch(tourUrl)
            .then(function(r) { return r.ok ? r.json() : null; })
            .then(function(tourDef) {
                if (!tourDef) return;

                // Check if already completed
                var completedKey = 'surgewave_tour_' + tourDef.id;
                var completed = localStorage.getItem(completedKey);

                // Create tour button
                var btn = document.createElement('button');
                btn.className = 'shepherd-tour-btn' + (completed ? ' shepherd-tour-completed' : '');
                btn.innerHTML = completed ? '&#10003; Tour' : '&#9654; Take a Tour';
                btn.title = tourDef.title || 'Start guided tour';
                document.body.appendChild(btn);

                btn.addEventListener('click', function() {
                    startTour(tourDef, completedKey);
                });

                // Auto-start on first visit if configured
                if (tourDef.triggerMode === 'auto-first-visit' && !completed) {
                    setTimeout(function() { startTour(tourDef, completedKey); }, 1000);
                }
            })
            .catch(function() { /* No tour for this page */ });
    });

    function startTour(tourDef, completedKey) {
        var tour = new Shepherd.Tour({
            useModalOverlay: true,
            defaultStepOptions: {
                classes: 'surgewave-shepherd-step',
                scrollTo: { behavior: 'smooth', block: 'center' },
                cancelIcon: { enabled: true }
            }
        });

        tourDef.steps.forEach(function(step, i) {
            var buttons = [];
            if (step.buttons) {
                step.buttons.forEach(function(b) {
                    buttons.push({
                        text: b.text,
                        action: tour[b.action] ? tour[b.action].bind(tour) : tour.next.bind(tour),
                        secondary: b.secondary || false
                    });
                });
            } else {
                if (i > 0) buttons.push({ text: 'Back', action: tour.back.bind(tour), secondary: true });
                if (i < tourDef.steps.length - 1) buttons.push({ text: 'Next', action: tour.next.bind(tour) });
                else buttons.push({ text: 'Done', action: tour.complete.bind(tour) });
            }

            var stepConfig = {
                id: step.id,
                title: step.title,
                text: step.text,
                buttons: buttons
            };

            if (step.attachTo && step.attachTo.element) {
                stepConfig.attachTo = { element: step.attachTo.element, on: step.attachTo.on || 'bottom' };
            }

            if (step.scrollTo !== undefined) stepConfig.scrollTo = step.scrollTo;

            tour.addStep(stepConfig);
        });

        tour.on('complete', function() {
            localStorage.setItem(completedKey, 'true');
            var btn = document.querySelector('.shepherd-tour-btn');
            if (btn) {
                btn.innerHTML = '&#10003; Tour';
                btn.classList.add('shepherd-tour-completed');
            }
        });

        tour.start();
    }
})();
